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
import Parser from "rss-parser"; 
import stringSimilarity from "string-similarity";

// --- 1. INITIALIZATION ---
dotenv.config();
const app = express();
const PORT = process.env.PORT || 4001;

// --- CONFIGURATION ---
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const MONGO_URI = process.env.MONGO_URI;
const TWITTER_API_IO_KEY = process.env.TWITTER_API_KEY;
const AWS_BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const AWS_REGION = process.env.AWS_REGION;

// --- TARGETS ---
// 1. Twitter Users to Auto-Fetch
const TARGET_HANDLES = [
//   "IndianTechGuide",
//   "bigtvtelugu",
//   "TeluguScribe",
  // "mufaddal_vohra",
];

// --- RSS SOURCES ---
const RSS_FEEDS = [
  { name: "NTV Telugu", url: "https://ntvtelugu.com/feed" },
  { name: "Google News", url: "https://news.google.com/rss?hl=te&gl=IN&ceid=IN:te" },
  { name: "ABP Telugu", url: "https://telugu.abplive.com/news/feed" },
  { name: "TV9 Telugu", url: "https://tv9telugu.com/feed" },
  { name: "V6 Telugu", url: "https://www.v6velugu.com/feed/" },
  { name: "10TV Telugu", url: "https://10tv.in/latest/feed" },
  { name: "Gulte", url: "https://telugu.gulte.com/feed" },
  { name: "Namasthe Telangana", url: "https://www.ntnews.com/feed" },
];

const s3Client = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const upload = multer({ storage: multer.memoryStorage() });

// Configure Parser to read custom fields (like media:content)
const rssParser = new Parser({
    customFields: {
        item: [
            ['media:content', 'mediaContent'], 
            ['media:thumbnail', 'mediaThumbnail'],
            ['content:encoded', 'contentEncoded']
        ]
    }
});

// --- VALIDATION ---
if (!GEMINI_API_KEY || !MONGO_URI || !TWITTER_API_IO_KEY) {
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
  promptType: { type: String, default: "DETAILED" }, 
  useAuthorContext: { type: Boolean, default: true },
  originalDbId: { type: mongoose.Schema.Types.ObjectId, default: null },
  queuedAt: { type: Date, default: Date.now },
});
const Queue = mongoose.models.Queue || mongoose.model("Queue", queueSchema);

const tagSchema = new mongoose.Schema({
    name: { type: String, required: true, unique: true },
    slug: { type: String, required: true, unique: true },
}, { timestamps: true });
const Tag = mongoose.models.Tag || mongoose.model("Tag", tagSchema);

const postSchema = new mongoose.Schema({
    postId: { type: Number, unique: true },
    title: { type: String, required: true },
    summary: String,
    text: String,
    url: { type: String, unique: true, sparse: true },
    imageUrl: String,
    videoUrl: String,
    media: [{
        mediaType: { type: String, default: "image" },
        url: String,
        width: Number,
        height: Number,
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
    isAINews: { type: Boolean, default: false },
    type: { type: String, default: "normal_post" },
    lang: { type: String, default: "te" },
}, { timestamps: true, collection: "posts" });

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

// Get Handle
const getHandleFromUrl = (url) => {
  if (!url) return null;
  const match = url.match(/(?:twitter\.com|x\.com)\/([^\/]+)/);
  return match ? match[1] : null;
};

async function getOrCreateTags(tagNames) {
  if (!tagNames || !Array.isArray(tagNames)) return [];
  const tagIds = [];
  for (const name of tagNames) {
    const slug = name.toLowerCase().replace(/ /g, "-").replace(/[^\w-]+/g, "");
    try {
      let tag = await Tag.findOne({ slug });
      if (!tag) tag = await Tag.create({ name, slug });
      tagIds.push(tag._id);
    } catch (e) { console.error(`Tag Error: ${e.message}`); }
  }
  return tagIds;
}

// ✅ SCRAPER
async function scrapeUrlContent(url) {
  if (!url || url.includes("twitter.com") || url.includes("x.com")) return null;
  try {
    const { data } = await axios.get(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" },
      timeout: 5000,
    });
    const $ = cheerio.load(data);
    $("script, style, nav, footer, header, aside, iframe, .ads").remove();
    let content = "";
    $('article, [itemprop="articleBody"], .post-content, .story-content').find("p").each((i, el) => {
       content += $(el).text().trim() + "\n";
    });
    if(!content) {
        $("p").each((i, el) => { if($(el).text().length > 30) content += $(el).text().trim() + "\n"; });
    }
    return content.substring(0, 15000).trim();
  } catch (err) {
    console.error(`⚠️ Scrape Failed: ${err.message}`);
    return null;
  }
}

// ✅ HELPER: Extract Image from RSS Item
function extractRSSImage(item) {
    if (item.enclosure && item.enclosure.url) return item.enclosure.url;
    if (item.mediaContent && item.mediaContent.$ && item.mediaContent.$.url) return item.mediaContent.$.url;
    if (item.mediaThumbnail && item.mediaThumbnail.$ && item.mediaThumbnail.$.url) return item.mediaThumbnail.$.url;
    if (item.contentEncoded || item.content) {
        const html = item.contentEncoded || item.content;
        const $ = cheerio.load(html);
        const firstImg = $('img').first().attr('src');
        if (firstImg) return firstImg;
    }
    return null;
}

// --- CORE FETCHING LOGIC ---

// ✅ 1. TWITTER FETCH & QUEUE
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
    tweets = tweets.slice(0, 5); // Limit to top 5

    if (tweets.length === 0) return 0;

    // Deduplication
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

// ✅ 2. RSS FETCH & QUEUE (With Deduplication)
async function fetchAndQueueRSS() {
    console.log("📡 RSS: Starting Fetch Cycle...");
    let totalQueued = 0;

    // Get Recent Data for Dedupe
    const recentPosts = await Post.find({
        publishedAt: { $gte: new Date(Date.now() - 48 * 60 * 60 * 1000) }
    }).select('title url source');
    const recentQueue = await Queue.find().select('text url');

    const existingTitles = [
        ...recentPosts.map(p => p.title),
        ...recentQueue.map(q => q.text.split('\n')[0].replace("Title: ", ""))
    ].filter(Boolean);

    const existingUrls = new Set([
        ...recentPosts.map(p => p.url),
        ...recentQueue.map(q => q.url)
    ]);

    for (const feedSource of RSS_FEEDS) {
        try {
            console.log(`   Scanning: ${feedSource.name}...`);
            const feed = await rssParser.parseURL(feedSource.url);
            const items = feed.items.slice(0, 5); 

            for (const item of items) {
                if (existingUrls.has(item.link)) continue;

                let isDuplicate = false;
                if (existingTitles.length > 0) {
                    const matches = stringSimilarity.findBestMatch(item.title, existingTitles);
                    if (matches.bestMatch.rating > 0.6) {
                        console.log(`   Skipping Duplicate: ${item.title}`);
                        isDuplicate = true;
                    }
                }

                if (!isDuplicate) {
                    const extractedImage = extractRSSImage(item);
                    
                    // Create Media Object for Worker
                    const mediaObj = extractedImage ? [{
                        type: 'photo',
                        media_url_https: extractedImage,
                        url: extractedImage
                    }] : [];

                    const newItem = new Queue({
                        id: new mongoose.Types.ObjectId().toString(),
                        text: `Title: ${item.title}\nSummary: ${item.contentSnippet || ""}`,
                        url: item.link,
                        imageUrl: extractedImage, 
                        media: mediaObj,
                        extendedEntities: { media: mediaObj }, 
                        source: feedSource.name,
                        user: { name: feedSource.name, screen_name: "RSS_Feed" },
                        postType: "normal_post",
                        promptType: "NEWS_ARTICLE",
                        queuedAt: new Date()
                    });

                    await newItem.save();
                    existingTitles.push(item.title);
                    existingUrls.add(item.link);
                    totalQueued++;
                }
            }
        } catch (err) {
            console.error(`   ❌ Failed to fetch ${feedSource.name}: ${err.message}`);
        }
    }
    console.log(`📡 RSS: Cycle Complete. Queued ${totalQueued} new items.`);
    return totalQueued;
}

// ✅ GEMINI PROMPT
async function formatTweetWithGemini(text, tweetUrl, sourceName) {
  const scrapedContext = tweetUrl ? await scrapeUrlContent(tweetUrl) : null;

  const prompt = `
    Role: Professional Telugu News Editor.
    Task: Rewrite the input into a high-quality Telugu news article.
    
    Input Title/Snippet: "${text}"
    ${scrapedContext ? `Full Article Context: ${scrapedContext}` : ""}

    REQUIREMENTS:
    1. Language: Native Telugu.
    2. Tone: Formal, Factual.
    3. Categorization: Assign ONE: [Politics, Cinema, Sports, Crime, Business, Technology, General].

    OUTPUT JSON FORMAT (Strict):
    {
      "title": "Telugu title catchy (Max 8 words)",
      "summary": "Detailed summary explaining the 'What', 'Where', and 'Why'. (60-80 words)",
      "category": "English Category Name"
    }
  `;

  try {
    const result = await model.generateContent(prompt);
    let textResp = result.response.text().replace(/```json|```/g, "").trim();
    return JSON.parse(textResp);
  } catch (e) {
    console.error("Gemini Error:", e.message);
    return null;
  }
}

// --- ROUTES ---

app.get("/", (req, res) => res.send("<h1>✅ Server Running: RSS + Twitter + Category</h1>"));

// 1. Trigger RSS Manually
app.get("/api/trigger-rss-fetch", async (req, res) => {
    const count = await fetchAndQueueRSS();
    res.json({ success: true, queued_count: count });
});

// 2. Trigger Twitter Auto-Fetch Manually
app.get("/api/trigger-auto-fetch", async (req, res) => {
  let total = 0;
  for (const handle of TARGET_HANDLES) {
    total += await fetchAndQueueTweetsForHandle(handle);
  }
  res.json({ success: true, queued_total: total });
});

// 3. Fetch Specific User Tweets (Manual)
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

    if (!response.ok) return res.status(response.status).json({ error: await response.text() });

    const data = await response.json();
    let tweets = data?.tweets ?? data?.data?.tweets ?? [];
    if (limit) tweets = tweets.slice(0, parseInt(limit));

    const postedIds = await Post.find({ tweetId: { $in: tweets.map((t) => t.id) } }).distinct("tweetId");
    const queuedIds = await Queue.find({ id: { $in: tweets.map((t) => t.id) } }).distinct("id");
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

// 4. Manual Post Queue (Array of Objects)
app.post("/api/add-rss-to-queue", async (req, res) => {
    try {
        const postsToQueue = Array.isArray(req.body) ? req.body : req.body.items;
        if (!Array.isArray(postsToQueue)) return res.status(400).json({ error: "Array required" });

        const newQueueDocs = postsToQueue.map(item => {
             // Mimic Twitter Media Structure for Manual Uploads
             const mediaObj = item.imageUrl ? [{
                type: 'photo',
                media_url_https: item.imageUrl, 
                url: item.imageUrl
            }] : [];

            return {
                id: new mongoose.Types.ObjectId().toString(),
                text: `Title: ${item.title}\nSummary: ${item.summary || ""}`,
                url: item.url,
                imageUrl: item.imageUrl,
                media: mediaObj,
                extendedEntities: { media: mediaObj }, // Crucial for Worker
                source: item.source || "Manual",
                promptType: "DETAILED",
                user: { name: "Manual", screen_name: "manual" },
                queuedAt: new Date()
            };
        });

        if(newQueueDocs.length > 0) await Queue.insertMany(newQueueDocs);
        res.json({ success: true, count: newQueueDocs.length });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// --- WORKER ---
cron.schedule("*/1 * * * *", async () => {
    const batch = await Queue.find().sort({ queuedAt: 1 }).limit(3);
    if (batch.length === 0) return;

    console.log(`⚙️ Worker: Processing ${batch.length} items...`);

    for (const item of batch) {
        try {
            console.log(`   Processing: ${item.url || item.id}`);
            
            const geminiData = await formatTweetWithGemini(item.text, item.url);

            if (geminiData) {
                // Image Handling
                let imageUrl = item.imageUrl;
                // If not in root, check the media array we created
                if(!imageUrl && item.extendedEntities?.media?.[0]) {
                    imageUrl = item.extendedEntities.media[0].media_url_https;
                }

                // Determine Post Type (Check for Video)
                let finalPostType = "normal_post";
                let tweetVideo = null;
                const mediaList = item.extendedEntities?.media || item.media || [];
                
                if (mediaList.length > 0 && mediaList[0].type === "video") {
                    const variants = mediaList[0].video_info?.variants || [];
                    const bestVideo = variants
                      .filter((v) => v.content_type === "video/mp4")
                      .sort((a, b) => b.bitrate - a.bitrate)[0];
                    if (bestVideo) {
                        tweetVideo = bestVideo.url;
                        finalPostType = "normal_video"; // Auto-promote to video post
                    }
                }

                const newPost = new Post({
                    postId: generatePostId(),
                    title: geminiData.title,
                    summary: geminiData.summary,
                    text: geminiData.summary,
                    url: item.url,
                    source: item.source || "Manual",
                    sourceName: item.user?.name || "Manual",
                    sourceType: item.source === "Manual" ? "manual" : "rss",
                    imageUrl: imageUrl, 
                    videoUrl: tweetVideo,
                    categories: [geminiData.category || "General"],
                    tags: [], 
                    publishedAt: new Date(),
                    isPublished: true,
                    type: finalPostType,
                    lang: "te"
                });

                await newPost.save();
                console.log(`   ✅ Published: [${geminiData.category}] ${geminiData.title}`);
                await Queue.deleteOne({ _id: item._id });
            } else {
                console.log("   ⚠️ Gemini Failed");
                await Queue.deleteOne({ _id: item._id });
            }
        } catch (e) {
            console.error(`   ❌ Error: ${e.message}`);
        }
        await sleep(5000);
    }
});

// --- SCHEDULERS ---

// 1. Fetch RSS every 30 mins
cron.schedule("*/30 * * * *", async () => {
    await fetchAndQueueRSS();
});

// 2. Fetch Twitter every 30 mins (Offset by 15 mins if needed, currently same time)
cron.schedule("*/30 * * * *", async () => {
    for (const handle of TARGET_HANDLES) {
        await fetchAndQueueTweetsForHandle(handle);
    }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));