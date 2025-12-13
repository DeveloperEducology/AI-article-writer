import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

async function test() {
    console.log("Testing available models...");
    const candidates = ["gemini-1.5-flash", "gemini-pro", "gemini-1.5-pro-latest"];
    
    for (const m of candidates) {
        try {
            console.log(`👉 Trying: ${m}`);
            const model = genAI.getGenerativeModel({ model: m });
            await model.generateContent("Hello");
            console.log(`   ✅ SUCCESS! Your code should use: "${m}"`);
            return; // Stop after finding a working one
        } catch (e) {
            console.log(`   ❌ Failed (${m}): ${e.message.split(':')[0]}`);
        }
    }
}

test();