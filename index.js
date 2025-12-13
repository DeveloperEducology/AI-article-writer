import express from "express";
import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
import cors from "cors";
import mongoose from "mongoose";
import cron from "node-cron";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

// --- 1. INITIALIZATION ---
dotenv.config();
const app = express();
const PORT = process.env.PORT || 4001;

// ESM Fix for __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- CONFIGURATION ---
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const MONGO_URI = process.env.MONGO_URI;
const TWITTER_API_IO_KEY = process.env.TWITTER_API_KEY;
const QUEUE_FILE = path.join(__dirname, "tweets_queue.json");

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

// --- 3. SCHEMAS (Updated to match your requirements) ---

// A. Helper Schemas
const stackedImageSchema = new mongoose.Schema({ url: String, caption: String });
const mediaSchema = new mongoose.Schema({ type: String, url: String });

const tagSchema = new mongoose.Schema({
    name: { type: String, required: true, unique: true },
    slug: { type: String, required: true, unique: true }
}, { timestamps: true });

const Tag = mongoose.models.Tag || mongoose.model("Tag", tagSchema);

// B. Main Post Schema
const postSchema = new mongoose.Schema(
  {
    postId: { type: Number, unique: true },
    title: { type: String, required: true, index: "text" },
    summary: { type: String, index: "text" },
    text: String, // Maps to 'content' from Gemini
    url: { type: String, unique: true, sparse: true }, // Maps to 'slug'
    imageFit: {
      type: String,
      enum: ["cover", "contain", "repeat", "stretch"],
      default: "contain",
    },
    imageUrl: String,
    stackedImages: [stackedImageSchema],
    relatedStories: [{ type: mongoose.Schema.Types.ObjectId, ref: "Post" }],
    source: {
      type: String,
      required: true,
      default: "Manual",
    },
    sourceType: {
      type: String,
      enum: ["rss", "manual", "tweet_api"], // We will use 'tweet_api'
      required: true,
      default: "manual",
    },
    publishedAt: { type: Date, default: Date.now, index: true},
    lang: { type: String, default: "te" },
    categories: [{ type: String, index: true, default: "General"  }],
    topCategory: { type: String, index: true },
    tags: [{ type: mongoose.Schema.Types.ObjectId, ref: "Tag", index: true }],
    isPublished: { type: Boolean, default: true, index: true },
    media: [mediaSchema],
    videoUrl: String,
    videoFit: {
      type: String,
      enum: ["COVER", "CONTAIN", "STRETCH"],
      default: "CONTAIN",
    },
    isBreaking: { type: Boolean, default: false },
    isTwitterLink: { type: Boolean, default: false }, // Will be true for these
    type: { type: String, default: "normal_post" },
    scheduledFor: { type: Date, default: null },
    tweetId: { type: String, unique: true, sparse: true },
    twitterUrl: String,
    pinnedIndex: { type: Number, default: null, index: true },
  },
  { timestamps: true, collection: "posts" }
);

const Post = mongoose.models.Post || mongoose.model("Post", postSchema);

// --- 4. GEMINI SETUP ---
const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ 
    model: "gemini-2.5-flash", // Or gemini-1.5-flash depending on availability
    generationConfig: { responseMimeType: "application/json" }
});

// --- HELPER FUNCTIONS ---

const readQueue = () => {
  if (!fs.existsSync(QUEUE_FILE)) return [];
  try {
    return JSON.parse(fs.readFileSync(QUEUE_FILE, "utf-8"));
  } catch (e) { return []; }
};

const writeQueue = (data) => {
  fs.writeFileSync(QUEUE_FILE, JSON.stringify(data, null, 2));
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const generatePostId = () => Math.floor(100000000 + Math.random() * 900000000);

// Helper: Convert string tags to ObjectIds
async function getOrCreateTags(tagNames) {
    if (!tagNames || !Array.isArray(tagNames)) return [];
    const tagIds = [];
    for (const name of tagNames) {
        const slug = name.toLowerCase().replace(/ /g, '-').replace(/[^\w-]+/g, '');
        try {
            let tag = await Tag.findOne({ slug: slug });
            if (!tag) {
                tag = await Tag.create({ name, slug });
            }
            tagIds.push(tag._id);
        } catch (e) {
            console.error(`⚠️ Error processing tag '${name}':`, e.message);
        }
    }
    return tagIds;
}

// Gemini Formatter
async function formatTweetWithGemini(text) {
  const prompt = `
    You are a professional Telugu news editor.
    Rewrite the following tweet into a news snippet.
    Output MUST be a valid JSON object with these exact keys:
    { 
      "title": "String (Telugu, max 8 words)", 
      "summary": "String (Telugu, min 60 words)", 
      "content": "String (Telugu, min 120 words)", 
      "slug_en": "String (English URL slug)", 
      "tags_en": ["String (English tags)"] 
    }
    Original Tweet: ${text}
  `;
  try {
    const result = await model.generateContent(prompt);
    const responseText = result.response.text();
    // In case model doesn't obey JSON mode strictly (though it should with config)
    return JSON.parse(responseText);
  } catch (e) {
    console.error("Gemini Error:", e.message);
    return null;
  }
}

// --- 5. API: FETCH & QUEUE ---
app.get("/api/fetch-user-last-tweets", async (req, res) => {
  const { userName } = req.query;
  if (!userName) return res.status(400).json({ error: "username required" });

  console.log(`📥 Fetching tweets for @${userName}...`);
  const API_URL = "https://api.twitterapi.io/twitter/user/last_tweets";

  try {
    const response = await fetch(`${API_URL}?userName=${userName}`, {
      headers: { "X-API-Key": TWITTER_API_IO_KEY },
    });
    
    if (!response.ok) {
        const txt = await response.text();
        console.error("Twitter API Error:", txt);
        return res.status(response.status).json({ error: txt });
    }

    const data = await response.json();
    let tweets = data?.tweets ?? data?.data?.tweets ?? [];

    if (tweets.length === 0) return res.json({ message: "No tweets found" });

    const currentQueue = readQueue();
    
    // Check against DB (using new Schema field: tweetId) AND Queue
    const existingDbIds = await Post.find({ tweetId: { $in: tweets.map(t => t.id) } }).distinct('tweetId');
    const queueIds = new Set(currentQueue.map(q => q.id));
    const dbIds = new Set(existingDbIds);

    const newTweets = tweets.filter(t => !dbIds.has(t.id) && !queueIds.has(t.id));

    if (newTweets.length === 0) {
      return res.json({ message: "All tweets already exist or queued." });
    }

    const updatedQueue = [...currentQueue, ...newTweets];
    writeQueue(updatedQueue);

    console.log(`✅ Queued ${newTweets.length} new tweets.`);
    res.json({ 
      success: true, 
      queued_count: newTweets.length, 
      message: "Tweets saved to local queue." 
    });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// --- 6. BACKGROUND WORKER ---
cron.schedule("*/1 * * * *", async () => {
  const queue = readQueue();
  if (queue.length === 0) return;

  console.log(`⚙️ Worker: Processing batch of ${queue.length} items...`);

  // Process only 1 at a time to be safe with Rate Limits, or small batch
  const tweet = queue[0]; // Take first item
  const remaining = queue.slice(1);

  try {
    console.log(`   Processing Tweet ID: ${tweet.id}...`);

    const geminiData = await formatTweetWithGemini(tweet.text);

    if (geminiData) {
      
      // Handle Tags
      const tagIds = await getOrCreateTags(geminiData.tags_en);

      // Extract Media (Image) from Tweet if available
      const tweetImage = tweet.extendedEntities?.media?.[0]?.media_url_https || 
                         tweet.media?.[0]?.media_url_https || null;

      const newPost = new Post({
        postId: generatePostId(),
        title: geminiData.title,
        summary: geminiData.summary,
        text: geminiData.content,
        url: geminiData.slug_en ? `${geminiData.slug_en}-${Date.now()}` : `post-${Date.now()}`,
        
        // Metadata
        source: "Manual",
        categories: ["General"],
        sourceType: "tweet_api",
        isTwitterLink: true,
        tweetId: tweet.id,
        twitterUrl: tweet.url,
        
        // Media & Tags
        imageUrl: tweetImage,
        tags: tagIds,
        lang: 'te',
        
        // Defaults
        publishedAt: new Date(),
        isPublished: true,
        topCategory: "Social",
        type: "normal_post"
      });

      await newPost.save();
      console.log(`   ✅ Saved to DB: ${geminiData.title.substring(0, 20)}...`);
      
      // Update Queue file only on success (remove processed item)
      writeQueue(remaining);
      // Rate Limiting
       await sleep(10000); 

    } else {
      console.log(`   ❌ Gemini failed (empty response), moving to back of queue.`);
      // Move to end of queue to retry later or discard based on your logic
      writeQueue([...remaining, tweet]); 
    }

  } catch (err) {
    console.error(`   ❌ Error processing ${tweet.id}:`, err.message);
    // On error, decide if you want to keep it in queue or discard. 
    // Here we discard to prevent blocking the queue forever.
    writeQueue(remaining); 
  }
  
  // Sleep slightly before next cron tick if needed, though cron controls timing.
});

// --- 7. START SERVER ---
app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
  console.log(`📂 Queue File: ${QUEUE_FILE}`);
});