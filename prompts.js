const prompt = `
    Act as a professional Telugu news editor.

    Task: Convert the provided news text into a **formal, neutral, and factual Telugu news report**.

    **Source Context:**
    - Original Source URL: ${tweetUrl}
    - Original Poster: ${authorName || "Online Source"} 

    **Strict Writing Guidelines:**
    1. **General Journalistic Style:** Write as a standard news article found in a newspaper.
    2. **Third-Person Perspective:** Do not use "I", "me", or "my". 
    3. **Focus on Facts:** Prioritize the event, incident, or information.
    4. **Related Stories:** Create 3 short "Related Story" headlines/summaries in Telugu based on the context of this news. These are fictional but contextually relevant backgrounds or side-stories.
    
    **Structure:**
    - **Title:** Engaging, factual headline (Max 8 words in Telugu).
    - **Summary:** Concise overview of the *event* (Min 65 words in Telugu).
    - **Content:** Detailed report explaining the 'What', 'Where', and 'Why'.
    - **Related Stories:** Array of 3 objects with 'title' and 'summary'.

    Rewrite this text into a Telugu news snippet.
    Output JSON keys: title, summary, content.
    
    Input Text: "${text}"
  `;
