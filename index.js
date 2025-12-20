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
    
    // Renamed 'type' to 'mediaType' to avoid Mongoose keyword conflict
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

// ✅ NEW HELPER: Extract Handle from URL
const getHandleFromUrl = (url) => {
    if (!url) return null;
    // Matches x.com/username or twitter.com/username
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

// ✅ Image Processing & Upload Helper (KEPT FOR FUTURE USE)
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

// ✅ Gemini Formatter: STRICT JOURNALISTIC TONE
async function formatTweetWithGemini(text, authorName) {
  const prompt = `
    Role: Professional Telugu News Editor.
    
    Task: Convert the provided information into a formal, neutral, and factual Telugu news report.

    Strict Guidelines:
    1. **NO First-Person Perspective:** Do not use "I", "We", "My", or write as if you are the author of the tweet. Write in the third person (objective voice).
    2. **Neutral Tone:** The writing must be professional, unbiased, and suitable for a mainstream news portal.
    3. **Focus on Facts:** Describe the event, incident, or update clearly. If the tweet is an opinion, report it as "According to reports..." or "It is being discussed that...".
    4. **Structure:**
       - **Title:** Engaging but factual headline (Max 8 words in Telugu).
       - **Summary:** A concise overview of the news (Min 65 words in Telugu).
       - **Content:** The detailed report. Start with the context, explain the main event, and conclude with the significance or outcome.

    Input Data (Tweet):
    "${text}"

    Output JSON Format:
    {
      "title": "Telugu Title",
      "summary": "Telugu Summary",
      "content": "Telugu Content",
      "slug_en": "english-slug-for-url",
      "tags_en": ["tag1", "tag2"]
    }
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

// ---------------------------------------------------------
// ROUTE 1: Fetch Tweets by User & Queue
// ---------------------------------------------------------
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

    // Check duplicates
    const postedIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
    const queuedIds = await Queue.find({ id: { $in: tweets.map(t => t.id) } }).distinct('id');
    const ignoredIds = new Set([...postedIds, ...queuedIds]);
    const newTweets = tweets.filter(t => !ignoredIds.has(t.id));

    if (newTweets.length === 0) return res.json({ message: "All fetched tweets already exist or are queued." });

    // Insert
    const queueDocs = newTweets.map(t => {
        const userInfo = t.user || { screen_name: userName, name: userName };
        return {
            id: t.id,
            text: t.text,
            url: t.url,
            media: t.media || [],
            extendedEntities: t.extendedEntities || {},
            user: {
                name: userInfo.name,
                screen_name: userInfo.screen_name,
                profile_image_url_https: userInfo.profile_image_url_https
            },
            postType: postType
        };
    });

    await Queue.insertMany(queueDocs);
    console.log(`✅ Queued ${newTweets.length} new tweets from @${userName}`);
    res.json({ success: true, queued_count: newTweets.length });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------
// ROUTE 2: Fetch Specific Tweets by IDs
// ---------------------------------------------------------
app.get("/api/fetch-tweets-by-ids", async (req, res) => {
    const { tweet_ids, type, hasAuthor } = req.query; // Expecting comma-separated string
    const postType = type || "normal_post";
   
    if (!tweet_ids) {
      return res.status(400).json({ error: "tweet_ids required (comma separated)" });
    }
   
    console.log(`📥 Fetching specific tweet IDs: ${tweet_ids}`);
    const API_URL = "https://api.twitterapi.io/twitter/tweets";
   
    try {
      const response = await fetch(`${API_URL}?tweet_ids=${tweet_ids}`, {
        headers: { "X-API-Key": TWITTER_API_IO_KEY },
      });
      
      if (!response.ok) return res.status(response.status).json({ error: await response.text() });
   
      const data = await response.json();
      const tweets = data?.tweets ?? [];
   
      if (tweets.length === 0) return res.json({ message: "No tweets found for provided IDs" });
   
      const postedIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
      const queuedIds = await Queue.find({ id: { $in: tweets.map(t => t.id) } }).distinct('id');
      const ignoredIds = new Set([...postedIds, ...queuedIds]);
      const newTweets = tweets.filter(t => !ignoredIds.has(t.id));
   
      if (newTweets.length === 0) return res.json({ message: "All provided tweets already exist or are queued." });
   
      // --- Prepare Queue Documents ---
      const queueDocs = newTweets.map(t => {
          // ✅ TRY TO GET USER INFO FROM OBJECT, ELSE EXTRACT FROM URL
          let screenName = t.user?.screen_name;
          let name = t.user?.name;

          if (!screenName) {
              const handle = getHandleFromUrl(t.url);
              if (handle) {
                  screenName = handle;
                  name = handle; // Use handle as name if name missing
              } else {
                  screenName = "Unknown";
                  name = "Twitter User";
              }
          }

          const userInfo = {
              name: name,
              screen_name: screenName,
              profile_image_url_https: t.user?.profile_image_url_https
          };
          
          return {
              id: t.id,
              text: t.text,
              url: t.url,
              media: t.media || [],
              extendedEntities: t.extendedEntities || {},
              user: userInfo,
              postType: postType
          };
      });
   
      await Queue.insertMany(queueDocs);
   
      console.log(`✅ Queued ${newTweets.length} specific tweets.`);
      res.json({ 
          success: true, 
          queued_count: newTweets.length, 
          type_assigned: postType 
      });
   
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: e.message });
    }
});
// ---------------------------------------------------------
// ROUTE 3: Manual Image Upload
// ---------------------------------------------------------
app.post("/api/upload", upload.single("file"), async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: "No file uploaded" });
      const s3Url = await processBufferAndUpload(req.file.buffer, "uploads", "manual");
      res.json({ url: s3Url });
    } catch (error) {
      console.error("❌ Upload Endpoint Error:", error);
      res.status(500).json({ error: "Image upload failed" });
    }
});

// --- 6. CRON WORKER ---

cron.schedule("*/1 * * * *", async () => {
  const batch = await Queue.find().sort({ queuedAt: 1 }).limit(3);
  
  if (batch.length === 0) return;

  console.log(`⚙️ Worker: Processing ${batch.length} items...`);

  for (const tweet of batch) {
    try {
        console.log(`   Processing Tweet ID: ${tweet.id}...`);

        // ✅ FINAL AUTHOR CHECK: User Object vs URL Extraction
        let authorHandle = tweet.user?.screen_name;
        let authorDisplayName = tweet.user?.name;

        // If handle is missing or generic, try extracting from URL again (Safety Net)
        if (!authorHandle || authorHandle === "Unknown" || authorHandle === "Twitter User") {
            const extracted = getHandleFromUrl(tweet.url);
            if (extracted) {
                authorHandle = extracted;
                // If the display name is also generic, update it to the handle
                if (!authorDisplayName || authorDisplayName === "Twitter User") {
                    authorDisplayName = extracted;
                }
            }
        }

        const authorName = authorDisplayName 
            ? `${authorDisplayName} (@${authorHandle})` 
            : "Social Media User";

        const geminiData = await formatTweetWithGemini(tweet.text, authorName);

        if (geminiData) {
            const tagIds = await getOrCreateTags(geminiData.tags_en);
            
            // --- MULTI-MEDIA PROCESSING ---
            let mediaArray = [];
            let mainImageUrl = null;
            const mediaEntities = tweet.extendedEntities?.media || tweet.media || [];
            const photoEntities = mediaEntities.filter(m => m.type === 'photo');

            if (photoEntities.length > 0) {
                console.log(`   🎨 Found ${photoEntities.length} photos. processing...`);
                for (const [index, mediaItem] of photoEntities.entries()) {
                    try {
                        const originalUrl = mediaItem.media_url_https;
                        
                        console.log(`      Using Direct Twitter URL for photo ${index + 1}: ${originalUrl}`);
                        mediaArray.push({
                            mediaType: 'image',
                            url: originalUrl, 
                            width: mediaItem.original_info?.width || mediaItem.sizes?.large?.w || 0,
                            height: mediaItem.original_info?.height || mediaItem.sizes?.large?.h || 0
                        });

                        if (index === 0) mainImageUrl = originalUrl;

                    } catch (imgErr) {
                        console.error(`      ⚠️ Photo ${index + 1} Failed: ${imgErr.message}`);
                    }
                }
            }

            let tweetVideo = null;
            if (mediaEntities.length > 0 && mediaEntities[0].type === 'video' && mediaEntities[0].video_info?.variants) {
                 const variants = mediaEntities[0].video_info.variants;
                 const bestVideo = variants
                    .filter(v => v.content_type === "video/mp4")
                    .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0];
                if (bestVideo) tweetVideo = bestVideo.url; 
            }

            const finalPostType = tweetVideo ? (tweet.postType || "normal_post") : "normal_post";

            const newPost = new Post({
                postId: generatePostId(),
                title: geminiData.title,
                summary: geminiData.summary,
                text: geminiData.content,
                url: '',
                source: "Twitter",
                sourceName: authorName, 
                sourceType: "twitter",
                isTwitterLink: true,
                tweetId: tweet.id,
                twitterUrl: tweet.url,
                imageUrl: mainImageUrl, 
                videoUrl: tweetVideo,
                media: mediaArray, 
                tags: tagIds,
                categories: ["General"],
                lang: 'te',
                publishedAt: new Date(),
                isPublished: true,
                type: finalPostType 
            });

            await newPost.save();
            console.log(`   ✅ Saved: ${geminiData.title.substring(0, 20)}... | Author: ${authorName}`);
            await Queue.deleteOne({ _id: tweet._id });
        } else {
            console.log(`   ⚠️ Gemini failed. Removing from queue.`);
            await Queue.deleteOne({ _id: tweet._id });
        }

    } catch (err) {
        console.error(`   ❌ Error: ${err.message}`);
    }

    if (batch.length > 1) await sleep(6000); 
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
