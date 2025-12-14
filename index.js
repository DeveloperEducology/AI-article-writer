import express from "express";
import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
import cors from "cors";
import mongoose from "mongoose";
import cron from "node-cron";
import path from "path";
import { fileURLToPath } from "url";
// ❌ REMOVED: import fs from "fs"; 

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

// ✅ NEW: Queue Schema (Replaces JSON file)
const queueSchema = new mongoose.Schema({
  id: { type: String, required: true, unique: true },
  text: String,
  url: String,
  media: Array,
  extendedEntities: Object,
  queuedAt: { type: Date, default: Date.now }
});
const Queue = mongoose.models.Queue || mongoose.model("Queue", queueSchema);

// Existing Schemas...
const tagSchema = new mongoose.Schema({
    name: { type: String, required: true, unique: true },
    slug: { type: String, required: true, unique: true }
}, { timestamps: true });
const Tag = mongoose.models.Tag || mongoose.model("Tag", tagSchema);

const postSchema = new mongoose.Schema({
    postId: { type: Number, unique: true },
    title: { type: String, required: true },
    summary: String,
    text: String,
    url: { type: String, unique: true, sparse: true },
    imageUrl: String,
    source: { type: String, default: "Manual" },
    sourceType: { type: String, default: "manual" },
    tweetId: { type: String, unique: true, sparse: true },
    twitterUrl: String,
    categories: [{ type: String, default: "General" }],
    tags: [{ type: mongoose.Schema.Types.ObjectId, ref: "Tag" }],
    publishedAt: { type: Date, default: Date.now },
    isPublished: { type: Boolean, default: true },
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
    Output JSON keys: title(min telugu 8 words), summary(min telugu 65 words), content, slug_en, tags_en.
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
  const { userName } = req.query;
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

    if (tweets.length === 0) return res.json({ message: "No tweets found" });

    // 1. Get IDs already in Posts
    const postedIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
    
    // 2. Get IDs already in Queue
    const queuedIds = await Queue.find({ id: { $in: tweets.map(t => t.id) } }).distinct('id');
    
    const ignoredIds = new Set([...postedIds, ...queuedIds]);

    // 3. Filter new tweets
    const newTweets = tweets.filter(t => !ignoredIds.has(t.id));

    if (newTweets.length === 0) {
      return res.json({ message: "All tweets already exist or queued." });
    }

    // 4. Save to MongoDB Queue (Insert Many)
    // We map tweet data to match our Schema structure if needed, or store raw
    const queueDocs = newTweets.map(t => ({
        id: t.id,
        text: t.text,
        url: t.url,
        media: t.media || [],
        extendedEntities: t.extendedEntities || {}
    }));

    await Queue.insertMany(queueDocs);

    console.log(`✅ Queued ${newTweets.length} new tweets to MongoDB.`);
    res.json({ success: true, queued_count: newTweets.length });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});


cron.schedule("*/1 * * * *", async () => {
  // 1. Fetch oldest 3 items from MongoDB Queue
  const batch = await Queue.find().sort({ queuedAt: 1 }).limit(3);
  
  if (batch.length === 0) return;

  console.log(`⚙️ Worker: Processing ${batch.length} items...`);

  for (const tweet of batch) {
    try {
        console.log(`   Processing Tweet ID: ${tweet.id}...`);

        const geminiData = await formatTweetWithGemini(tweet.text);

        if (geminiData) {
            const tagIds = await getOrCreateTags(geminiData.tags_en);
            const tweetImage = tweet.extendedEntities?.media?.[0]?.media_url_https || 
                               tweet.media?.[0]?.media_url_https || null;

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
                tags: tagIds,
                categories: ["General"],
                lang: 'te',
                publishedAt: new Date(),
                isPublished: true,
                type: "normal_post"
            });

            await newPost.save();
            console.log(`   ✅ Saved: ${geminiData.title.substring(0, 20)}...`);
            
            // ✅ Remove from Queue after success
            await Queue.deleteOne({ _id: tweet._id });
        } else {
            console.log(`   ⚠️ Gemini failed. Removing from queue to prevent block.`);
            // Optionally: Increment a retry counter instead of deleting
            await Queue.deleteOne({ _id: tweet._id });
        }

    } catch (err) {
        console.error(`   ❌ Error: ${err.message}`);
    }

    // Rate Limit Wait
    if (batch.length > 1) await sleep(6000); 
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
