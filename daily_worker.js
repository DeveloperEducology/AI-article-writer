require('dotenv').config();
const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
const { GoogleGenerativeAI } = require("@google/generative-ai");

// --- CONFIGURATION ---
const INPUT_FILE = 'daily_input.json';
MONGODB_URI= "mongodb+srv://vijaymarka:admin123@cluster0.ivjiolu.mongodb.net/News?retryWrites=true&w=majority"

// Initialize Gemini
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || "");
const model = genAI.getGenerativeModel({
    model: "gemini-2.5-flash",
    generationConfig: { responseMimeType: "application/json" }
});

const SYSTEM_PROMPT = `
Rewrite the following article in Telugu language.
Output MUST be a valid JSON object:
{ 
  "title": "String (max 8 words)", 
  "summary": "String (min 65 words)", 
  "content": "String (min 150 words)", 
  "slug_en": "String (English URL slug)",
  "tags_en": ["String"] 
}

Original Article:
`;

// --- DATABASE SCHEMAS ---

// 1. Mock Schemas (These were referenced in your postSchema but not provided)
// Replace these with your actual schema definitions if different.
const stackedImageSchema = new mongoose.Schema({ url: String, caption: String });
const mediaSchema = new mongoose.Schema({ type: String, url: String });

const tagSchema = new mongoose.Schema({
    name: { type: String, required: true, unique: true },
    slug: { type: String, required: true, unique: true }
}, { timestamps: true });

// 2. Tag Model
const Tag = mongoose.models.Tag || mongoose.model("Tag", tagSchema);

// 3. Post Schema (Your provided schema)
const postSchema = new mongoose.Schema(
    {
        postId: { type: Number, unique: true },
        title: { type: String, required: true, index: "text" },
        summary: { type: String, index: "text" },
        text: String, // Maps to 'content' from AI
        url: { type: String, unique: true, sparse: true }, // Maps to 'slug_en'
        imageFit: {
            type: String,
            enum: ["cover", "contain", "repeat", "stretch"],
            default: "cover",
        },
        imageUrl: String,
        stackedImages: [stackedImageSchema],
        relatedStories: [{ type: mongoose.Schema.Types.ObjectId, ref: "Post" }],
        source: String,
        sourceType: {
            type: String,
            enum: ["rss", "manual", "tweet_api"],
            required: true,
            default: "manual", // Default to manual since this is a script
        },
        publishedAt: { type: Date, default: Date.now, index: true },
        lang: { type: String, default: 'te' }, // Default to Telugu based on prompt
        categories: [{ type: String, index: true }],
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
        isTwitterLink: { type: Boolean, default: false },
        type: { type: String, default: "normal_post" },
        scheduledFor: { type: Date, default: null },
        tweetId: { type: String, unique: true, sparse: true },
        twitterUrl: String,
        pinnedIndex: { type: Number, default: null, index: true },
    },
    { timestamps: true, collection: "posts" }
);

const Post = mongoose.models.Post || mongoose.model("Post", postSchema);

// --- HELPER FUNCTIONS ---

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Helper to generate a random 9-digit Post ID (or replace with your logic)
const generatePostId = () => Math.floor(100000000 + Math.random() * 900000000);

// Helper to Find or Create Tags and return ObjectIds
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
            console.error(`   ⚠️ Error processing tag '${name}':`, e.message);
        }
    }
    return tagIds;
}

// --- MAIN LOGIC ---

async function runBatch() {
    try {
        // 1. Connect to Database
        console.log("🔌 Connecting to MongoDB...");
        await mongoose.connect(MONGODB_URI);
        console.log("✅ DB Connected.");

        const inputPath = path.join(__dirname, INPUT_FILE);
        
        // 2. Validate Input
        if (!fs.existsSync(inputPath)) {
            console.error(`❌ ERROR: Could not find '${INPUT_FILE}'`);
            createExampleFile(inputPath);
            process.exit(1);
        }

        console.log(`📖 Reading articles from ${INPUT_FILE}...`);
        const rawData = fs.readFileSync(inputPath, 'utf-8');
        let articles;
        
        try {
            articles = JSON.parse(rawData);
            if (!Array.isArray(articles)) throw new Error("Input is not a JSON Array");
        } catch (e) {
            console.error("❌ ERROR: daily_input.json is not valid.");
            process.exit(1);
        }

        console.log(`🚀 Found ${articles.length} items. Starting processing...`);

        // 3. Process Loop
        for (const [index, item] of articles.entries()) {
            
            // Normalize Input
            let articleText = "";
            let sourceUrl = "";
            
            if (typeof item === 'string') {
                articleText = item;
            } else if (typeof item === 'object' && item !== null) {
                articleText = item.content || item.text || item.body || "";
                sourceUrl = item.url || "";
            }

            if (!articleText || articleText.length < 10) {
                console.log(`   ⚠️ [${index + 1}/${articles.length}] Skipped (Content too short)`);
                continue;
            }

            console.log(`   ⚡ [${index + 1}/${articles.length}] Generating content with Gemini...`);
            
            try {
                // A. Generate Content
                const result = await model.generateContent(SYSTEM_PROMPT + articleText);
                const responseText = result.response.text();
                const jsonResponse = JSON.parse(responseText);
                
                // B. Handle Tags (Convert strings to ObjectIds)
                const tagObjectIds = await getOrCreateTags(jsonResponse.tags_en);

                // C. Create DB Object
                const newPost = new Post({
                    postId: generatePostId(), // Unique Number
                    title: jsonResponse.title,
                    summary: jsonResponse.summary,
                    text: jsonResponse.content,
                    url: jsonResponse.slug_en ? `${jsonResponse.slug_en}-${Date.now()}` : `post-${Date.now()}`, // Ensure URL uniqueness
                    source: "Manual",
                    sourceType: 'manual', // Generated via script
                    lang: 'te', // Telugu
                    tags: tagObjectIds,
                    isPublished: true,
                    categories: ["General"],
                    topCategory: "General", // Default category or extract from AI
                    publishedAt: new Date()
                });

                // D. Save to MongoDB
                await newPost.save();
                console.log(`      ✅ Saved to DB: "${jsonResponse.title.substring(0, 30)}..."`);

            } catch (err) {
                console.error(`      ❌ Failed: ${err.message}`);
            }

            // Rate Limiting
            if (index < articles.length - 1) await sleep(10000); 
        }

        console.log("\n🎉 Batch processing complete.");

    } catch (error) {
        console.error("Fatal Error:", error);
    } finally {
        await mongoose.disconnect();
        console.log("🔌 DB Disconnected.");
    }
}

// Helper to create dummy file
function createExampleFile(filepath) {
    const example = [
        { "url": "http://google.com", "content": "Paste article text here..." }
    ];
    fs.writeFileSync(filepath, JSON.stringify(example, null, 2));
    console.log(`👉 Created example '${INPUT_FILE}'.`);
}

runBatch();