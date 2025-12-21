import express from "express";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
import cors from "cors";
import mongoose from "mongoose";
import cron from "node-cron";
import path from "path";
import { fileURLToPath } from "url";
import axios from "axios";
import sharp from "sharp";
import multer from "multer";

// --- 1. INITIALIZATION ---
dotenv.config();
const app = express();
const PORT = process.env.PORT || 4001;

// ESM Fix
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- CONFIGURATION ---
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const MONGO_URI = process.env.MONGO_URI;
const TWITTER_API_IO_KEY = process.env.TWITTER_API_KEY;

// AWS Config
const AWS_BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const AWS_REGION = process.env.AWS_REGION;
const s3Client = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Multer Config (Memory Storage for Sharp Processing)
const upload = multer({ storage: multer.memoryStorage() });

// --- VALIDATION ---
if (!GEMINI_API_KEY || !MONGO_URI || !TWITTER_API_IO_KEY || !process.env.AWS_ACCESS_KEY_ID) {
  console.error("❌ CRITICAL ERROR: Missing keys in .env file.");
  process.exit(1);
}

// --- 2. DB CONNECTION ---
mongoose.connect(MONGO_URI)
  .then(() => console.log("✅ MongoDB Connected"))
  .catch((err) => console.error("❌ MongoDB Error:", err));

app.use(cors());
app.use(express.json({ limit: '50mb' }));

// --- 3. SCHEMAS ---

// Queue Schema
const queueSchema = new mongoose.Schema({
  id: { type: String, required: true, unique: true },
  text: String,
  url: String,
  media: Array,
  extendedEntities: Object,
  user: Object, 
  postType: { type: String, default: "normal_post" },
  useAuthorContext: { type: Boolean, default: true },
  queuedAt: { type: Date, default: Date.now }
});
const Queue = mongoose.models.Queue || mongoose.model("Queue", queueSchema);

// Tag Schema
const tagSchema = new mongoose.Schema({
    name: { type: String, required: true, unique: true },
    slug: { type: String, required: true, unique: true }
}, { timestamps: true });
const Tag = mongoose.models.Tag || mongoose.model("Tag", tagSchema);

// Post Schema
const postSchema = new mongoose.Schema({
    postId: { type: Number, unique: true },
    title: { type: String, required: true },
    summary: String,
    text: String,
    url: { type: String, unique: true, sparse: true },
    imageUrl: String, 
    videoUrl: String,
    
    media: [{ 
        mediaType: { type: String, default: 'image' }, 
        url: String,
        width: Number,
        height: Number
    }], 
    sourceName: String, 
    
    source: { type: String, default: "Manual" },
    sourceType: { type: String, default: "manual" },
    tweetId: { type: String, unique: true, sparse: true },
    twitterUrl: String,
    categories: [{ type: String, default: "General" }],
    tags: [{ type: mongoose.Schema.Types.ObjectId, ref: "Tag" }],
    publishedAt: { type: Date, default: Date.now },
    isPublished: { type: Boolean, default: true },
    type: { type: String, default: "normal_post" },
    lang: { type: String, default: "te" }
}, { timestamps: true, collection: "posts" });

const Post = mongoose.models.Post || mongoose.model("Post", postSchema);

// --- 4. GEMINI SETUP ---
const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ 
    model: "gemini-2.5-flash", 
    generationConfig: { responseMimeType: "application/json" }
});

// --- HELPER FUNCTIONS ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const generatePostId = () => Math.floor(100000000 + Math.random() * 900000000);

const getHandleFromUrl = (url) => {
    if (!url) return null;
    const match = url.match(/(?:twitter\.com|x\.com)\/([^\/]+)/);
    return match ? match[1] : null;
};

async function getOrCreateTags(tagNames) {
    if (!tagNames || !Array.isArray(tagNames)) return [];
    const tagIds = [];
    for (const name of tagNames) {
        const slug = name.toLowerCase().replace(/ /g, '-').replace(/[^\w-]+/g, '');
        try {
            let tag = await Tag.findOne({ slug });
            if (!tag) tag = await Tag.create({ name, slug });
            tagIds.push(tag._id);
        } catch (e) { console.error(`Tag Error: ${e.message}`); }
    }
    return tagIds;
}

async function processBufferAndUpload(buffer, folder = "posts", slug = "image") {
    try {
        const optimizedBuffer = await sharp(buffer)
            .resize({ width: 1080, withoutEnlargement: true })
            .webp({ quality: 80 })
            .toBuffer();

        const fileName = `${folder}/${slug}-${Date.now()}.webp`;

        const command = new PutObjectCommand({
            Bucket: AWS_BUCKET_NAME,
            Key: fileName,
            Body: optimizedBuffer,
            ContentType: "image/webp",
        });

        await s3Client.send(command);

        return `https://${AWS_BUCKET_NAME}.s3.${AWS_REGION}.amazonaws.com/${fileName}`;

    } catch (error) {
        console.error("❌ S3 Upload Helper Error:", error);
        throw error;
    }
}

// ✅ REUSABLE TWEET FETCHER
const TARGET_HANDLES = ['IndianTechGuide', 'bigtvtelugu', 'TeluguScribe', 'mufaddal_vohra'];

async function fetchAndQueueTweetsForHandle(userName) {
    console.log(`🤖 Auto-Fetch: Checking @${userName}...`);
    const API_URL = "https://api.twitterapi.io/twitter/user/last_tweets";

    try {
        const response = await fetch(`${API_URL}?userName=${userName}`, {
            headers: { "X-API-Key": TWITTER_API_IO_KEY },
        });

        if (!response.ok) {
            console.error(`❌ API Error for @${userName}: ${response.status}`);
            return 0;
        }

        const data = await response.json();
        let tweets = data?.tweets ?? data?.data?.tweets ?? [];
        
        tweets = tweets.slice(0, 5); 

        if (tweets.length === 0) return 0;

        // Duplicate Check
        const postedIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
        const queuedIds = await Queue.find({ id: { $in: tweets.map(t => t.id) } }).distinct('id');
        const ignoredIds = new Set([...postedIds, ...queuedIds]);
        const newTweets = tweets.filter(t => !ignoredIds.has(t.id));

        if (newTweets.length === 0) return 0;

        const queueDocs = newTweets.map(t => ({
            id: t.id,
            text: t.text,
            url: t.url,
            media: t.media || [],
            extendedEntities: t.extendedEntities || {},
            user: t.user || { screen_name: userName, name: userName },
            postType: "normal_post",
            // We set this to false effectively in logic, but keeping DB consistency
            useAuthorContext: false 
        }));

        await Queue.insertMany(queueDocs);
        console.log(`✅ Auto-Fetch: Queued ${newTweets.length} from @${userName}`);
        return newTweets.length;

    } catch (error) {
        console.error(`❌ Auto-Fetch Error for @${userName}:`, error.message);
        return 0;
    }
}

// ✅ GEMINI FORMATTER: STRICT GENERAL JOURNALISTIC STYLE
async function formatTweetWithGemini() {
  
  // Notice: We do NOT toggle logic based on authorName anymore.
  // We strictly ask for a General News Report.

  const prompt = `
    Act as a professional Telugu news editor.

Write a factual Telugu news article in a neutral, journalistic tone, similar in style to mainstream Telugu news portals.

Follow this structure strictly:

1. Start with a strong opening paragraph that gives background or context to the incident/event.
2. Clearly describe the main incident with accurate facts and names.
3. Highlight any unusual, rare, or surprising aspect of the event.
4. Include political or administrative reactions if relevant, but avoid speculation or opinionated language.
5. End with a concluding line that summarizes the significance of the event.

Writing rules:
- Language must be pure Telugu (simple, reader-friendly).
- Sentence length should be short to medium.
- No exaggeration, no clickbait.
- Do not add assumptions or unverified claims.
- Maintain neutrality and factual accuracy.

    **Structure:**
    - **Title:** Engaging, factual headline (Max 8 words in Telugu).
    - **Summary:** Concise overview of the *event* (Min 65 words in Telugu).
    - **Content:** Detailed report explaining the 'What', 'Where', and 'Why'.

    Rewrite this text into a Telugu news snippet.
    Output JSON keys: title, summary, content, slug_en, tags_en.
  `;

  try {
    const result = await model.generateContent(prompt);
    return JSON.parse(result.response.text());
  } catch (e) {
    return null;
  }
}

// --- 5. ROUTES ---

app.get("/", (req, res) => res.send("<h1>✅ Server Running (MongoDB + S3 + Auto Queue)</h1>"));

// ROUTE 1: Fetch Tweets by User & Queue
app.get("/api/fetch-user-last-tweets", async (req, res) => {
  const { userName, limit, type } = req.query;
  const postType = type || "normal_post";

  if (!userName) return res.status(400).json({ error: "username required" });

  console.log(`📥 Fetching tweets for @${userName}...`);
  const API_URL = "https://api.twitterapi.io/twitter/user/last_tweets";

  try {
    const response = await fetch(`${API_URL}?userName=${userName}`, {
      headers: { "X-API-Key": TWITTER_API_IO_KEY },
    });
    
    if (!response.ok) return res.status(response.status).json({ error: await response.text() });

    const data = await response.json();
    let tweets = data?.tweets ?? data?.data?.tweets ?? [];

    if (limit) {
        const limitNum = parseInt(limit);
        if (!isNaN(limitNum) && limitNum > 0) tweets = tweets.slice(0, limitNum);
    }

    if (tweets.length === 0) return res.json({ message: "No tweets found" });

    const postedIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
    const queuedIds = await Queue.find({ id: { $in: tweets.map(t => t.id) } }).distinct('id');
    const ignoredIds = new Set([...postedIds, ...queuedIds]);
    const newTweets = tweets.filter(t => !ignoredIds.has(t.id));

    if (newTweets.length === 0) return res.json({ message: "All fetched tweets already exist or are queued." });

    const queueDocs = newTweets.map(t => ({
        id: t.id,
        text: t.text,
        url: t.url,
        media: t.media || [],
        extendedEntities: t.extendedEntities || {},
        user: t.user || { screen_name: userName, name: userName },
        postType: postType,
        useAuthorContext: false // Force General Context
    }));

    await Queue.insertMany(queueDocs);
    console.log(`✅ Queued ${newTweets.length} new tweets from @${userName}`);
    res.json({ success: true, queued_count: newTweets.length });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ROUTE 2: Fetch Specific Tweets by IDs
app.get("/api/fetch-tweets-by-ids", async (req, res) => {
    const { tweet_ids, type } = req.query; 
    const postType = type || "normal_post";
    // We ignore authorContext param now as we force general style
  
    if (!tweet_ids) return res.status(400).json({ error: "tweet_ids required" });
  
    const API_URL = "https://api.twitterapi.io/twitter/tweets";
  
    try {
      const response = await fetch(`${API_URL}?tweet_ids=${tweet_ids}`, {
        headers: { "X-API-Key": TWITTER_API_IO_KEY },
      });
      
      if (!response.ok) return res.status(response.status).json({ error: await response.text() });
  
      const data = await response.json();
      const tweets = data?.tweets ?? [];
  
      if (tweets.length === 0) return res.json({ message: "No tweets found" });
  
      const postedIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
      const queuedIds = await Queue.find({ id: { $in: tweets.map(t => t.id) } }).distinct('id');
      const ignoredIds = new Set([...postedIds, ...queuedIds]);
      const newTweets = tweets.filter(t => !ignoredIds.has(t.id));
  
      if (newTweets.length === 0) return res.json({ message: "All provided tweets already exist or are queued." });
  
      const queueDocs = newTweets.map(t => {
          let screenName = t.user?.screen_name;
          let name = t.user?.name;
          if (!screenName) {
              const handle = getHandleFromUrl(t.url);
              if (handle) { screenName = handle; name = handle; } 
              else { screenName = "Unknown"; name = "Twitter User"; }
          }
          return {
              id: t.id,
              text: t.text,
              url: t.url,
              media: t.media || [],
              extendedEntities: t.extendedEntities || {},
              user: { name, screen_name: screenName, profile_image_url_https: t.user?.profile_image_url_https },
              postType: postType,
              useAuthorContext: false // Force General Context
          };
      });
  
      await Queue.insertMany(queueDocs);
      res.json({ success: true, queued_count: newTweets.length });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
});

// ROUTE 3: Manual Image Upload
app.post("/api/upload", upload.single("file"), async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: "No file uploaded" });
      const s3Url = await processBufferAndUpload(req.file.buffer, "uploads", "manual");
      res.json({ url: s3Url });
    } catch (error) {
      res.status(500).json({ error: "Image upload failed" });
    }
});

// ROUTE 4: Create Manual Posts (Bulk)
app.post("/api/create-manual-posts", async (req, res) => {
    try {
        const postsArray = req.body;
        if (!Array.isArray(postsArray) || postsArray.length === 0) {
            return res.status(400).json({ error: "Input must be a non-empty array." });
        }
        const newPosts = postsArray.map(post => ({
            postId: generatePostId(),
            title: post.title,
            summary: post.summary,
            text: post.content || post.summary, 
            imageUrl: post.imageUrl || null,
            videoUrl: post.videoUrl || null,
            source: post.source || "Manual",
            sourceType: "manual",
            categories: Array.isArray(post.categories) ? post.categories : [post.categories || "General"],
            isPublished: true,
            publishedAt: new Date(),
            type: post.type || "normal_post",
            lang: "te"
        }));
        await Post.insertMany(newPosts);
        res.json({ success: true, count: newPosts.length });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ROUTE 5: Manually Trigger Auto-Fetch
app.get("/api/trigger-auto-fetch", async (req, res) => {
    console.log("🚀 Manually triggering auto-fetch...");
    const results = {};
    for (const handle of TARGET_HANDLES) {
        const count = await fetchAndQueueTweetsForHandle(handle);
        results[handle] = count;
    }
    res.json({ success: true, results });
});


// --- 6. CRON WORKERS ---

// CRON 1: Process Queue (Runs every 1 minute)
// --- 6. CRON WORKERS ---

// CRON 1: Process Queue (Runs every 1 minute)
cron.schedule("*/1 * * * *", async () => {
  const batch = await Queue.find().sort({ queuedAt: 1 }).limit(3);
  if (batch.length === 0) return;

  console.log(`⚙️ Worker: Processing ${batch.length} items...`);

  for (const tweet of batch) {
    try {
        console.log(`   Processing Tweet ID: ${tweet.id}...`);

        // 1. Extract Author Name (if available)
        let authorHandle = tweet.user?.screen_name;
        let authorDisplayName = tweet.user?.name;

        if (!authorHandle || authorHandle === "Unknown" || authorHandle === "Twitter User") {
            const extracted = getHandleFromUrl(tweet.url);
            if (extracted) {
                authorHandle = extracted;
                if (!authorDisplayName || authorDisplayName === "Twitter User") authorDisplayName = extracted;
            }
        }

        // 2. Determine Prompt Context
        // We construct author name ONLY for DB Source storage, NOT for the prompt context
        let dbSourceAuthor = null;
        if (authorHandle && authorHandle !== "Unknown" && authorHandle !== "Twitter User") {
            dbSourceAuthor = authorDisplayName ? `${authorDisplayName} (@${authorHandle})` : `@${authorHandle}`;
        }
        
        // Pass to Gemini (Strict General News Mode)
        const geminiData = await formatTweetWithGemini(tweet.text, tweet.url, dbSourceAuthor);

        if (geminiData) {
            const tagIds = await getOrCreateTags(geminiData.tags_en);
            
            // --- 3. Process Images ---
            let mediaArray = [];
            let mainImageUrl = null;
            const mediaEntities = tweet.extendedEntities?.media || tweet.media || [];
            const photoEntities = mediaEntities.filter(m => m.type === 'photo');

            if (photoEntities.length > 0) {
                for (const [index, mediaItem] of photoEntities.entries()) {
                    const originalUrl = mediaItem.media_url_https;
                    mediaArray.push({
                        mediaType: 'image',
                        url: originalUrl, 
                        width: mediaItem.original_info?.width || mediaItem.sizes?.large?.w || 0,
                        height: mediaItem.original_info?.height || mediaItem.sizes?.large?.h || 0
                    });
                    if (index === 0) mainImageUrl = originalUrl;
                }
            }

            // --- 4. Process Video ---
            let tweetVideo = null;
            if (mediaEntities.length > 0 && mediaEntities[0].type === 'video' && mediaEntities[0].video_info?.variants) {
                 const variants = mediaEntities[0].video_info.variants;
                 // Find best MP4
                 const bestVideo = variants
                    .filter(v => v.content_type === "video/mp4")
                    .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0]; // Descending bitrate
                
                if (bestVideo) tweetVideo = bestVideo.url; 
            }

            // --- 5. Determine Post Type (UPDATED LOGIC) ---
            let finalPostType = "normal_post";

            if (tweetVideo) {
                // If video detected:
                // 1. If the requested type is just 'normal_post' (default), upgrade to 'normal_video'
                // 2. If it's something specific (e.g. 'breaking_news'), keep it.
                finalPostType = (tweet.postType === "normal_post") ? "normal_video" : tweet.postType;
            } else {
                // No video -> force normal_post
                finalPostType = "normal_post";
            }

            // Save Source Name
            const dbSourceName = dbSourceAuthor || "Twitter";

            const newPost = new Post({
                postId: generatePostId(),
                title: geminiData.title,
                summary: geminiData.summary,
                text: geminiData.content,
                url: '',
                source: "Twitter",
                sourceName: dbSourceName, 
                sourceType: "twitter",
                isTwitterLink: true,
                tweetId: tweet.id,
                twitterUrl: tweet.url,
                imageUrl: mainImageUrl, 
                videoUrl: tweetVideo,
                media: mediaArray, 
                tags: tagIds,
                categories: ["General"],
                publishedAt: new Date(),
                isPublished: true,
                type: finalPostType, // ✅ Uses the corrected video logic
                lang: "te"
            });

            await newPost.save();
            console.log(`   ✅ Saved: ${geminiData.title.substring(0, 20)}... | Type: ${finalPostType}`);
            await Queue.deleteOne({ _id: tweet._id });
        } else {
            console.log(`   ⚠️ Gemini failed.`);
            await Queue.deleteOne({ _id: tweet._id });
        }
    } catch (err) {
        console.error(`   ❌ Error: ${err.message}`);
    }
    if (batch.length > 1) await sleep(6000); 
  }
});

// CRON 2: Auto-Fetch Tweets (Runs every 30 minutes)
cron.schedule("*/30 * * * *", async () => {
    console.log("⏰ CRON: Starting scheduled auto-fetch for target handles...");
    for (const handle of TARGET_HANDLES) {
        await fetchAndQueueTweetsForHandle(handle);
        await sleep(2000); 
    }
    console.log("⏰ CRON: Auto-fetch cycle complete.");
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
