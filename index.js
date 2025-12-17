import express from "express";
import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
import cors from "cors";
import mongoose from "mongoose";
import cron from "node-cron";
import path from "path";
import { fileURLToPath } from "url";

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

// --- VALIDATION ---
if (!GEMINI_API_KEY || !MONGO_URI || !TWITTER_API_IO_KEY) {
  console.error("❌ CRITICAL ERROR: Missing keys in .env file.");
  process.exit(1);
}

// --- 2. DB CONNECTION ---
mongoose.connect(MONGO_URI)
  .then(() => console.log("✅ MongoDB Connected"))
  .catch((err) => console.error("❌ MongoDB Error:", err));

app.use(cors());
app.use(express.json({ limit: '10mb' }));

// --- 3. SCHEMAS ---

// Queue Schema
const queueSchema = new mongoose.Schema({
  id: { type: String, required: true, unique: true },
  text: String,
  url: String,
  media: Array,
  extendedEntities: Object,
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

async function formatTweetWithGemini(text) {
  const prompt = `
    You are a professional Telugu news editor.
    Rewrite this tweet into a telugu news snippet.
    Output JSON keys: title(max telugu 8 words), summary(min telugu 65 words), content, slug_en, tags_en.
    Tweet: ${text}
  `;
  try {
    const result = await model.generateContent(prompt);
    return JSON.parse(result.response.text());
  } catch (e) {
    return null;
  }
}

// --- 5. ROUTES ---

app.get("/", (req, res) => res.send("<h1>✅ Server Running (MongoDB Queue)</h1>"));

// API: Fetch & Queue
app.get("/api/fetch-user-last-tweets", async (req, res) => {
  const { userName, limit, type } = req.query;
  const postType = type || "normal_post";

  if (!userName) return res.status(400).json({ error: "username required" });

  console.log(`📥 Fetching tweets for @${userName} (Type: ${postType})...`);
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
        if (!isNaN(limitNum) && limitNum > 0) {
            tweets = tweets.slice(0, limitNum);
            console.log(`✂️ Limit applied: Processing only top ${limitNum} tweets.`);
        }
    }

    if (tweets.length === 0) return res.json({ message: "No tweets found" });

    const postedIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
    const queuedIds = await Queue.find({ id: { $in: tweets.map(t => t.id) } }).distinct('id');
    const ignoredIds = new Set([...postedIds, ...queuedIds]);
    const newTweets = tweets.filter(t => !ignoredIds.has(t.id));

    if (newTweets.length === 0) {
      return res.json({ message: "All fetched tweets already exist or are queued." });
    }

    const queueDocs = newTweets.map(t => ({
        id: t.id,
        text: t.text,
        url: t.url,
        media: t.media || [],
        extendedEntities: t.extendedEntities || {},
        postType: postType
    }));

    await Queue.insertMany(queueDocs);

    console.log(`✅ Queued ${newTweets.length} new tweets as '${postType}'.`);
    res.json({ 
        success: true, 
        queued_count: newTweets.length, 
        requested_limit: limit || "All",
        type_assigned: postType 
    });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
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

        const geminiData = await formatTweetWithGemini(tweet.text);

        if (geminiData) {
            const tagIds = await getOrCreateTags(geminiData.tags_en);
            
            // Image Extraction
            const tweetImage = tweet.extendedEntities?.media?.[0]?.media_url_https || 
                               tweet.media?.[0]?.media_url_https || null;

            // Video Extraction Logic
            let tweetVideo = null;
            if (tweet.extendedEntities?.media?.[0]?.video_info?.variants) {
                const variants = tweet.extendedEntities.media[0].video_info.variants;
                // Filter for mp4 and sort by bitrate (descending)
                const bestVideo = variants
                    .filter(v => v.content_type === "video/mp4")
                    .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0];
                
                if (bestVideo) tweetVideo = bestVideo.url;
            }

            // ✅ TYPE FALLBACK LOGIC
            // If video is missing, force type to "normal_post" regardless of what was requested
            const finalPostType = tweetVideo ? (tweet.postType || "normal_post") : "normal_post";

            const newPost = new Post({
                postId: generatePostId(),
                title: geminiData.title,
                summary: geminiData.summary,
                text: geminiData.content,
                url: geminiData.slug_en ? `${geminiData.slug_en}-${Date.now()}` : `post-${Date.now()}`,
                source: "Manual",
                sourceType: "tweet_api",
                isTwitterLink: true,
                tweetId: tweet.id,
                twitterUrl: tweet.url,
                imageUrl: tweetImage,
                videoUrl: tweetVideo,
                tags: tagIds,
                categories: ["General"],
                lang: 'te',
                publishedAt: new Date(),
                isPublished: true,
                type: finalPostType // ✅ Uses the calculated type
            });

            await newPost.save();
            console.log(`   ✅ Saved: ${geminiData.title.substring(0, 20)}... (Type: ${finalPostType})`);
            
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
