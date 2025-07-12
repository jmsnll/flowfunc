# NLP Text Summarization Example

This example demonstrates a comprehensive NLP pipeline that processes text through multiple stages including tokenization, keyword extraction, summarization, and sentiment analysis.

## Overview

The workflow performs the following steps:
1. **Tokenize Text**: Breaks down input text into words, removing stopwords and non-alphabetic characters
2. **Extract Keywords**: Identifies the most common keywords from the tokenized text
3. **Generate Summary**: Creates summaries using the original text and extracted keywords
4. **Analyze Sentiment**: Performs sentiment analysis on the generated summaries
5. **Aggregate Results**: Combines all sentiment analysis results into a final report

## Files

- `workflow.yaml` - The workflow definition
- `main.py` - The Python module containing all the NLP processing logic

## Setup

1. Install dependencies (from the project root):
   ```bash
   uv sync
   ```

2. Run the workflow (always from the project root):
   ```bash
   uv run flowfunc run examples/nlp_text_summarization/workflow.yaml
   ```

> **Note:** Always run from the project root so Python can find the `examples` module. Running from inside the example folder will cause import errors.

## How it Works

The workflow processes multiple text inputs through a sophisticated NLP pipeline:

1. **Parallel Processing**: Each text input is processed independently through the first four steps
2. **Multi-step Analysis**: Text flows through tokenization → keyword extraction → summarization → sentiment analysis
3. **Final Aggregation**: All sentiment results are combined into a comprehensive report

## Configuration

The workflow includes three sample texts with different sentiments:
- Positive review: "The movie was excellent!..."
- Negative review: "The movie was bad and boring..."
- Neutral review: "An alright film with a good sense of humor..."

You can modify the `text` parameter in the workflow to analyze different content.

## Output

The workflow produces a `final_sentiment_report.json` file containing aggregated sentiment analysis for all processed texts.
