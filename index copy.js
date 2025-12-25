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
import * as cheerio from "cheerio";

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

// Multer Config
const upload = multer({ storage: multer.memoryStorage() });

// --- VALIDATION ---
if (
  !GEMINI_API_KEY ||
  !MONGO_URI ||
  !TWITTER_API_IO_KEY ||
  !process.env.AWS_ACCESS_KEY_ID
) {
  console.error("❌ CRITICAL ERROR: Missing keys in .env file.");
  process.exit(1);
}

// --- 2. DB CONNECTION ---
mongoose
  .connect(MONGO_URI)
  .then(() => console.log("✅ MongoDB Connected"))
  .catch((err) => console.error("❌ MongoDB Error:", err));

app.use(cors());
app.use(express.json({ limit: "50mb" }));

// --- 3. SCHEMAS ---

const queueSchema = new mongoose.Schema({
  id: { type: String, required: true, unique: true },
  text: String,
  url: String,
  media: Array,
  imageUrl: String,
  extendedEntities: Object,
  source: { type: String, default: "Manual" },
  user: Object,
  postType: { type: String, default: "normal_post" },
  
  // ✅ NEW FIELD: Stores the key (e.g., 'CRIME', 'BREAKING')
  promptType: { type: String, default: "DETAILED" }, 
  
  useAuthorContext: { type: Boolean, default: true },
  originalDbId: { type: mongoose.Schema.Types.ObjectId, default: null },
  queuedAt: { type: Date, default: Date.now },
});
const Queue = mongoose.models.Queue || mongoose.model("Queue", queueSchema);

const tagSchema = new mongoose.Schema(
  {
    name: { type: String, required: true, unique: true },
    slug: { type: String, required: true, unique: true },
  },
  { timestamps: true }
);
const Tag = mongoose.models.Tag || mongoose.model("Tag", tagSchema);

const postSchema = new mongoose.Schema(
  {
    postId: { type: Number, unique: true },
    title: { type: String, required: true },
    summary: String,
    text: String,
    url: { type: String, unique: true, sparse: true },
    imageUrl: String,
    videoUrl: String,
    // ✅ NEW FIELD: Related Stories
    relatedStories: [
      {
        title: String,
        summary: String,
        imageUrl: String,
        url: String,
      },
    ],

    media: [
      {
        mediaType: { type: String, default: "image" },
        url: String,
        width: Number,
        height: Number,
      },
    ],
    sourceName: String,
    source: { type: String, default: "Manual" },
    sourceType: { type: String, default: "manual" },
    tweetId: { type: String, unique: true, sparse: true },
    twitterUrl: String,
    categories: [{ type: String, default: "General" }],
    tags: [{ type: mongoose.Schema.Types.ObjectId, ref: "Tag" }],
    // REMOVED relatedStories field
    publishedAt: { type: Date, default: Date.now },
    isPublished: { type: Boolean, default: true },
    isAINews: { type: Boolean, default: false },
    type: { type: String, default: "normal_post" },
    lang: { type: String, default: "te" },

  },
  { timestamps: true, collection: "posts" }
);

const Post = mongoose.models.Post || mongoose.model("Post", postSchema);

// --- 4. GEMINI SETUP ---
const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);

const model = genAI.getGenerativeModel({
  model: "gemini-2.5-flash-lite",
  generationConfig: { responseMimeType: "application/json" },
});

// --- HELPER FUNCTIONS ---
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
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
    const slug = name
      .toLowerCase()
      .replace(/ /g, "-")
      .replace(/[^\w-]+/g, "");
    try {
      let tag = await Tag.findOne({ slug });
      if (!tag) tag = await Tag.create({ name, slug });
      tagIds.push(tag._id);
    } catch (e) {
      console.error(`Tag Error: ${e.message}`);
    }
  }
  return tagIds;
}

// ✅ HELPER: Scrape URL Content (Optimized for News Sites)
async function scrapeUrlContent(url) {
  if (!url) return null;
  if (
    url.includes("twitter.com") ||
    url.includes("x.com") ||
    url.includes("youtube.com")
  )
    return null;

  try {
    console.log(`🔗 Scraping context from: ${url}`);
    const { data } = await axios.get(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
      },
      timeout: 5000,
    });

    const $ = cheerio.load(data);

    // 1. REMOVE CLUTTER
    const junkSelectors = [
      "script",
      "style",
      "nav",
      "footer",
      "header",
      "aside",
      "iframe",
      ".ads",
      ".advertisement",
      ".sidebar",
      ".menu",
      ".navigation",
      ".related-posts",
      ".read-more",
      ".comments",
      ".social-share",
      ".breadcrumb",
      "#header",
      "#footer",
      ".meta-data",
      ".tag-cloud",
      ".author-bio",
      ".newsletter-signup",
      ".video-player",
      ".latest-news",
      ".trending",
      ".must-read",
      ".web-stories",
      ".top-nav",
      ".bottom-nav",
      ".more-videos",
    ];

    $(junkSelectors.join(", ")).remove();

    // 2. TARGET CONTENT
    const contentSelectors = [
      "article",
      '[itemprop="articleBody"]',
      ".article-body",
      ".post-content",
      ".story-content",
      ".main-content",
      "#content-body",
    ];

    let content = "";
    let foundSpecificContainer = false;

    for (const selector of contentSelectors) {
      if ($(selector).length > 0) {
        $(selector)
          .find("p, h2, h3, li")
          .each((i, el) => {
            const text = $(el).text().trim();
            if (text.length > 20) content += text + "\n";
          });
        foundSpecificContainer = true;
        break;
      }
    }

    // 3. FALLBACK
    if (!foundSpecificContainer) {
      $("p").each((i, el) => {
        const text = $(el).text().trim();
        if (text.length > 30) content += text + "\n";
      });
    }

    // 4. CLEANUP
    content = content
      .replace(/Also Watch:[\s\S]*/gi, "")
      .replace(/Read Also:[\s\S]*/gi, "")
      .replace(/మరిన్ని వీడియోల కోసం.*$/gim, "")
      .replace(/Click here for more/gi, "");

    return content.substring(0, 15000).trim();
  } catch (err) {
    console.error(`⚠️ Scrape Failed: ${err.message}`);
    return null;
  }
}

async function processBufferAndUpload(
  buffer,
  folder = "posts",
  slug = "image"
) {
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
const TARGET_HANDLES = [
  "IndianTechGuide",
  "bigtvtelugu",
  "TeluguScribe",
  // "mufaddal_vohra",
];

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
    const postedIds = await Post.find({
      tweetId: { $in: tweets.map((t) => t.id) },
    }).distinct("tweetId");
    const queuedIds = await Queue.find({
      id: { $in: tweets.map((t) => t.id) },
    }).distinct("id");
    const ignoredIds = new Set([...postedIds, ...queuedIds]);
    const newTweets = tweets.filter((t) => !ignoredIds.has(t.id));

    if (newTweets.length === 0) return 0;

    const queueDocs = newTweets.map((t) => ({
      id: t.id,
      text: t.text,
      url: t.url,
      media: t.media || [],
      extendedEntities: t.extendedEntities || {},
      user: t.user || { screen_name: userName, name: userName },
      postType: "normal_post",
      useAuthorContext: false,
    }));

    await Queue.insertMany(queueDocs);
    console.log(`✅ Auto-Fetch: Queued ${newTweets.length} from @${userName}`);
    return newTweets.length;
  } catch (error) {
    console.error(`❌ Auto-Fetch Error for @${userName}:`, error.message);
    return 0;
  }
}

// ✅ HELPER: Queue RSS Posts for Rewrite
const RSS_SOURCES = [
  { name: "NTV Telugu" }, 
  { name: "TV9 Telugu" },
  { name: "10TV Telugu" }
];

async function queueRSSPostsForRewrite() {
  console.log("📰 Scanner: Checking RSS posts...");
  try {
    const sourceNames = RSS_SOURCES.map((s) => s.name);
    const posts = await Post.find({
      source: { $in: sourceNames },
      sourceType: "rss",
    })
      .sort({ publishedAt: -1 })
      .limit(20);

    if (posts.length === 0) {
      console.log("✅ Scanner: No pending RSS posts.");
      return 0;
    }

    const queueDocs = [];
    const idsToUpdate = [];

    for (const post of posts) {
      queueDocs.push({
        id: post._id.toString(),
        text: `Title: ${post.title}\nSummary: ${post.summary || post.text}`,
        url: post.url,
        imageUrl: post.imageUrl,
        media: [],
        extendedEntities: {},
        publishedAt: new Date(),
        user: {
          name: post.source,
          screen_name: post.source.replace(/\s+/g, "_"),
        },
        postType: post.type || "normal_post",
        useAuthorContext: false,
        originalDbId: post._id,
        isAINews: true
      });
      idsToUpdate.push(post._id);
    }

    await Queue.insertMany(queueDocs);
    await Post.updateMany(
      { _id: { $in: idsToUpdate } },
      { $set: { sourceType: "rss_queued" } }
    );

    console.log(`🔄 Scanner: Queued ${posts.length} RSS posts.`);
    return posts.length;
  } catch (err) {
    console.error("❌ Scanner Error:", err.message);
    return 0;
  }
}

// ✅ GEMINI FORMATTER: UPDATED (Removed Related Stories)
async function formatTweetWithGemini(text, tweetUrl) {
  let scrapedContext = null;
  if (tweetUrl) {
    scrapedContext = await scrapeUrlContent(tweetUrl);
  }

 const prompt = `
You are a professional Telugu newspaper editor with experience in mainstream Telugu journalism.

========================
INPUT INFORMATION
========================
• Headline / Snippet:
"${text}"


${scrapedContext ? `
• Full Article Context (IMPORTANT):
Below is the complete article text collected from the source. 
You MUST read and understand the entire content and use it as the primary reference while writing the news article. 
Do NOT omit important facts, names, locations, or updates mentioned here.

${scrapedContext}
` : ""}

========================
STRICT WRITING RULES
========================
1. Writing Style:
   - Neutral, factual, third-person
   - Standard Telugu inshorts and way2news style, newspaper tone
   - No opinions, no exaggeration, no promotional language

2. Language:
   - Pure, native Telugu
   - Avoid English words unless absolutely unavoidable (e.g., film titles)
   - No slang or casual expressions

3. Structure (MANDATORY):
   - Title: Maximum 8 Telugu words
   - Summary: 60–80 words



========================
OUTPUT FORMAT (CRITICAL)
========================
• Output MUST be valid JSON only
• Do NOT add explanations or extra text
• Escape all control characters (\\n, \\t, quotes, etc.)
• No trailing commas

========================
REQUIRED JSON STRUCTURE
========================
{
  "title": "తెలుగు శీర్షిక",
  "summary": "65–80 పదాల వార్తా సారాంశం",
  ]
}
`;

  try {
    const result = await model.generateContent(prompt);
    let responseText = result.response.text();
    responseText = responseText
      .replace(/```json/g, "")
      .replace(/```/g, "")
      .trim();

    try {
      return JSON.parse(responseText);
    } catch (parseError) {
      console.warn(
        "⚠️ Standard JSON parse failed. Attempting to sanitize...",
        parseError.message
      );
      const sanitizedText = responseText
        .replace(/(?:\r\n|\r|\n)/g, "\\n")
        .replace(/\t/g, "\\t");
      try {
        return JSON.parse(sanitizedText);
      } catch (finalError) {
        console.error("❌ Gemini JSON Fix Failed. Raw Response:", responseText);
        return null;
      }
    }
  } catch (e) {
    console.error("❌ Gemini API Error:", e.message);
    if (e.response && e.response.promptFeedback) {
      console.error("   Block Reason:", e.response.promptFeedback);
    }
    return null;
  }
}

// --- 5. ROUTES ---

app.get("/", (req, res) =>
  res.send("<h1>✅ Server Running (Gemini 2.0 Flash + URL Context)</h1>")
);


// ✅ UPDATED ENDPOINT
app.post("/api/add-rss-to-queue", async (req, res) => {
  try {
    // Expecting: { items: [...], type: "CRIME" } OR items with individual types
    const { items, type } = req.body; 
    
    // Fallback if user sends array directly
    const postsToQueue = Array.isArray(req.body) ? req.body : items;
    const globalType = type || "DETAILED"; // Default prompt type

    if (!Array.isArray(postsToQueue) || postsToQueue.length === 0) {
      return res.status(400).json({ error: "Input must be an array of items." });
    }

    // ... (Deduplication Logic remains the same) ...
    const inputUrls = postsToQueue.map(item => item.url).filter(Boolean);
    const existingPosts = await Post.find({ url: { $in: inputUrls } }).distinct('url');
    const existingQueue = await Queue.find({ url: { $in: inputUrls } }).distinct('url');
    const ignoredUrls = new Set([...existingPosts, ...existingQueue]);

    const newQueueDocs = postsToQueue
      .filter(item => item.title && item.url && !ignoredUrls.has(item.url))
      .map(item => {
        const mediaObj = item.imageUrl ? [{
            type: 'photo',
            media_url_https: item.imageUrl, 
            url: item.imageUrl
        }] : [];

        return {
          id: new mongoose.Types.ObjectId().toString(),
          text: `Title: ${item.title}\nSummary: ${item.summary || ""}`,
          url: item.url,
          media: mediaObj,
          extendedEntities: { media: mediaObj }, 
          source: item.source || "Manual",
          
          // ✅ ASSIGN PROMPT TYPE
          // Prioritize item-level type, fallback to request-level type, then default
          promptType: (item.type || globalType).toUpperCase(), 

          user: {
            name: item.source || "Manual RSS",
            screen_name: (item.source || "manual").replace(/\s+/g, "_").toLowerCase(),
            profile_image_url_https: "" 
          },
          postType: "normal_post",
          queuedAt: new Date()
        };
      });

    if (newQueueDocs.length > 0) {
      await Queue.insertMany(newQueueDocs);
    }

    res.json({
      success: true,
      message: `Added ${newQueueDocs.length} items to queue with style: ${globalType}`,
      queuedCount: newQueueDocs.length,
    });

  } catch (error) {
    console.error("❌ Error:", error);
    res.status(500).json({ error: error.message });
  }
});

// ROUTE 1: Fetch Tweets by User
app.get("/api/fetch-user-last-tweets", async (req, res) => {
  const { userName, limit, type } = req.query;
  const postType = type || "normal_post";

  if (!userName) return res.status(400).json({ error: "username required" });

  try {
    const response = await fetch(
      `https://api.twitterapi.io/twitter/user/last_tweets?userName=${userName}`,
      {
        headers: { "X-API-Key": TWITTER_API_IO_KEY },
      }
    );

    if (!response.ok)
      return res.status(response.status).json({ error: await response.text() });

    const data = await response.json();
    let tweets = data?.tweets ?? data?.data?.tweets ?? [];
    if (limit) tweets = tweets.slice(0, parseInt(limit));

    const postedIds = await Post.find({
      tweetId: { $in: tweets.map((t) => t.id) },
    }).distinct("tweetId");
    const queuedIds = await Queue.find({
      id: { $in: tweets.map((t) => t.id) },
    }).distinct("id");
    const ignoredIds = new Set([...postedIds, ...queuedIds]);
    const newTweets = tweets.filter((t) => !ignoredIds.has(t.id));

    if (newTweets.length === 0) return res.json({ message: "No new tweets." });

    const queueDocs = newTweets.map((t) => ({
      id: t.id,
      text: t.text,
      url: t.url,
      media: t.media || [],
      extendedEntities: t.extendedEntities || {},
      user: t.user || { screen_name: userName, name: userName },
      postType: postType,
      useAuthorContext: false,
    }));

    await Queue.insertMany(queueDocs);
    res.json({ success: true, queued_count: newTweets.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ROUTE 2: Fetch Specific Tweets
app.get("/api/fetch-tweets-by-ids", async (req, res) => {
  const { tweet_ids, type } = req.query;
  const postType = type || "normal_post";
  if (!tweet_ids) return res.status(400).json({ error: "tweet_ids required" });

  try {
    const response = await fetch(
      `https://api.twitterapi.io/twitter/tweets?tweet_ids=${tweet_ids}`,
      {
        headers: { "X-API-Key": TWITTER_API_IO_KEY },
      }
    );
    if (!response.ok)
      return res.status(response.status).json({ error: await response.text() });
    const data = await response.json();
    const tweets = data?.tweets ?? [];

    const postedIds = await Post.find({
      tweetId: { $in: tweets.map((t) => t.id) },
    }).distinct("tweetId");
    const queuedIds = await Queue.find({
      id: { $in: tweets.map((t) => t.id) },
    }).distinct("id");
    const ignoredIds = new Set([...postedIds, ...queuedIds]);
    const newTweets = tweets.filter((t) => !ignoredIds.has(t.id));

    if (newTweets.length === 0) return res.json({ message: "No new tweets." });

    const queueDocs = newTweets.map((t) => ({
      id: t.id,
      text: t.text,
      url: t.url,
      media: t.media || [],
      extendedEntities: t.extendedEntities || {},
      user: t.user || { screen_name: "Unknown", name: "User" },
      postType: postType,
      useAuthorContext: false,
    }));

    await Queue.insertMany(queueDocs);
    res.json({ success: true, queued_count: newTweets.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ROUTE 3: Manual Upload
app.post("/api/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "No file" });
    const s3Url = await processBufferAndUpload(
      req.file.buffer,
      "uploads",
      "manual"
    );
    res.json({ url: s3Url });
  } catch (error) {
    res.status(500).json({ error: "Upload failed" });
  }
});

// ROUTE 4: Create Posts
app.post("/api/create-manual-posts", async (req, res) => {
  try {
    const postsArray = req.body;
    if (!Array.isArray(postsArray))
      return res.status(400).json({ error: "Array required" });
    const newPosts = postsArray.map((post) => ({
      postId: generatePostId(),
      title: post.title,
      summary: post.summary,
      text: post.content || post.summary,
      imageUrl: post.imageUrl || null,
      videoUrl: post.videoUrl || null,
      source: post.source || "Manual",
      sourceType: "manual",
      categories: Array.isArray(post.categories)
        ? post.categories
        : [post.categories || "General"],
      isPublished: true,
      publishedAt: new Date(),
      type: post.type || "normal_post",
      lang: "te",
    }));
    await Post.insertMany(newPosts);
    res.json({ success: true, count: newPosts.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Manual Triggers
app.get("/api/trigger-auto-fetch", async (req, res) => {
  for (const handle of TARGET_HANDLES)
    await fetchAndQueueTweetsForHandle(handle);
  res.json({ success: true });
});
app.get("/api/trigger-rss-rewrite", async (req, res) => {
  const count = await queueRSSPostsForRewrite();
  res.json({ success: true, count });
});

// --- 6. CRON WORKERS ---
cron.schedule("*/1 * * * *", async () => {
  const batch = await Queue.find().sort({ queuedAt: 1 }).limit(3);
  if (batch.length === 0) return;
  console.log(`⚙️ Worker: Processing ${batch.length} items...`);

  for (const item of batch) {
    try {
      console.log(`   Processing Queue ID: ${item.id}...`);

      // Determine Author Name
      let authorHandle = item.user?.screen_name;
      let authorDisplayName = item.user?.name;
      if (!authorHandle || authorHandle === "Unknown") {
        const extracted = getHandleFromUrl(item.url);
        if (extracted) {
          authorHandle = extracted;
          authorDisplayName = extracted;
        }
      }
      const dbSourceAuthor = authorHandle
        ? `${authorDisplayName} (@${authorHandle})`
        : "Twitter";

      // ✅ CALL GEMINI
      const geminiData = await formatTweetWithGemini(
        item.text,
        item.url,
        dbSourceAuthor
      );

      if (geminiData) {
        const tagIds = await getOrCreateTags(geminiData.tags_en);

        if (item.originalDbId) {
          // RSS UPDATE PATH (Existing RSS)
          const existingPost = await Post.findById(item.originalDbId);
          if (existingPost) {
            existingPost.title = geminiData.title;
            existingPost.summary = geminiData.summary;
            existingPost.text = geminiData.content;
            existingPost.source = "Manual";
            existingPost.sourceType = "manual";
            existingPost.tags = tagIds;
            await existingPost.save();
            console.log(
              `   ✅ UPDATED RSS Post: ${geminiData.title.substring(0, 20)}...`
            );
          }
        } else {
          // CREATE NEW PATH (Tweets AND Manual RSS Queue)
          let mediaArray = [];
          let mainImageUrl = item.imageUrl || null; // Use Queue imageUrl if present
          const mediaEntities =
            item.extendedEntities?.media || item.media || [];

          // Process Photos
          const photoEntities = mediaEntities.filter((m) => m.type === "photo");
          if (photoEntities.length > 0) {
            for (const [index, mediaItem] of photoEntities.entries()) {
              mediaArray.push({
                mediaType: "image",
                url: mediaItem.media_url_https,
                width: mediaItem.original_info?.width || 0,
                height: mediaItem.original_info?.height || 0,
              });
              if (!mainImageUrl && index === 0)
                mainImageUrl = mediaItem.media_url_https;
            }
          }

          // Process Video
          let tweetVideo = null;
          if (mediaEntities.length > 0 && mediaEntities[0].type === "video") {
            const variants = mediaEntities[0].video_info?.variants || [];
            const bestVideo = variants
              .filter((v) => v.content_type === "video/mp4")
              .sort((a, b) => b.bitrate - a.bitrate)[0];
            if (bestVideo) tweetVideo = bestVideo.url;
          }

          let finalPostType = "normal_post";
          if (tweetVideo)
            finalPostType =
              item.postType === "normal_post" ? "normal_video" : item.postType;

          const newPost = new Post({
            postId: generatePostId(),
            title: geminiData.title,
            summary: geminiData.summary,
            text: geminiData.content ? geminiData.content : '',
            url: item.url,
            source: "Manual",
            sourceName: dbSourceAuthor,
            sourceType: "manual",
            isTwitterLink: !!item.user?.screen_name, // True if came from Twitter
            tweetId: item.id && item.id.length < 24 ? item.id : undefined,
            twitterUrl: item.url,
            isAINews: true,
            imageUrl: mainImageUrl,
            videoUrl: tweetVideo,
            media: mediaArray,
            tags: tagIds,
            categories: ["General"],
            publishedAt: new Date(),
            isPublished: true,
            type: finalPostType,
            lang: "te",
          });
          await newPost.save();
          console.log(
            `   ✅ CREATED Post: ${geminiData.title.substring(0, 20)}...`
          );
        }
        await Queue.deleteOne({ _id: item._id });
      } else {
        console.log(`   ⚠️ Gemini returned NULL (Check logs).`);
        await Queue.deleteOne({ _id: item._id });
      }
    } catch (err) {
      console.error(`   ❌ Worker Error: ${err.message}`);
    }
    if (batch.length > 1) await sleep(6000);
  }
});

// CRON SCHEDULERS
cron.schedule("*/30 * * * *", async () => {
  for (const h of TARGET_HANDLES) await fetchAndQueueTweetsForHandle(h);
});
cron.schedule("*/30 * * * *", async () => {
  await queueRSSPostsForRewrite();
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
