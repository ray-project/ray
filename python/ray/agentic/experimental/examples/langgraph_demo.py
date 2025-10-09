"""
LangGraph + Ray Agentic Demo
=============================

LangGraph agent with distributed Ray tools using structured schemas.

Prerequisites:
    pip install langchain-openai langgraph pydantic
    export OPENAI_API_KEY="your-api-key"

Usage:
    python langgraph_demo.py
"""

import os
import sys
from typing import List

from pydantic import BaseModel, Field

import ray
from ray.agentic.experimental import AgentSession
from ray.agentic.experimental.adapters import LangGraphAdapter


# Pydantic models for structured inputs/outputs
class SalesAnalysisResult(BaseModel):
    """Sales analysis results."""

    region: str
    revenue: str
    growth: str
    top_product: str


class FeedbackAnalysisResult(BaseModel):
    """Customer feedback analysis results."""

    sentiment: str
    total_reviews: int
    avg_score: float
    key_themes: List[str]


class MarketTrendsResult(BaseModel):
    """Market trends analysis results."""

    industry: str
    growth_rate: str
    top_competitors: List[str]


class ForecastResult(BaseModel):
    """Revenue forecast results."""

    q1_2025: str
    q2_2025: str
    confidence: str


# Distributed tools with full Pydantic schemas
@ray.remote(num_cpus=4)
def analyze_sales_data(
    region: str = Field(
        description="The region to analyze, e.g., 'North America', 'Europe', 'Asia'"
    ),
) -> SalesAnalysisResult:
    """Analyze sales data for a specific region. Returns revenue, growth rate, and top products."""
    print(f"[Ray Task - 4 CPUs] Analyzing sales for {region}")
    return SalesAnalysisResult(
        region=region,
        revenue="$2.4M",
        growth="+18% YoY",
        top_product="Enterprise Plan",
    )


@ray.remote(memory=8 * 1024**3)
def process_customer_feedback(
    sentiment: str = Field(
        description="Sentiment filter: 'positive', 'negative', or 'neutral'"
    ),
) -> FeedbackAnalysisResult:
    """Process customer feedback filtered by sentiment. Returns review statistics and themes."""
    print(f"[Ray Task - 8GB RAM] Processing {sentiment} feedback")
    return FeedbackAnalysisResult(
        sentiment=sentiment,
        total_reviews=1250,
        avg_score=4.2,
        key_themes=["product quality", "support", "pricing"],
    )


@ray.remote
def fetch_market_trends(
    industry: str = Field(
        description="Industry sector, e.g., 'SaaS', 'FinTech', 'Healthcare'"
    ),
) -> MarketTrendsResult:
    """Fetch current market trends for an industry. Returns growth rate and competitors."""
    print(f"[Ray Task] Fetching trends for {industry}")
    return MarketTrendsResult(
        industry=industry,
        growth_rate="12% annually",
        top_competitors=["CompanyA", "CompanyB"],
    )


@ray.remote(num_cpus=2)
def generate_forecast(
    quarters: int = Field(
        description="Number of quarters to forecast, e.g., 2 for Q1-Q2"
    ),
) -> ForecastResult:
    """Generate revenue forecast for specified quarters. Returns predictions with confidence."""
    print(f"[Ray Task - 2 CPUs] Generating {quarters}-quarter forecast")
    return ForecastResult(
        q1_2025="$2.8M",
        q2_2025="$3.1M",
        confidence="85%",
    )


def main():
    # Check API key
    if not os.getenv("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY not set")
        print("Usage: export OPENAI_API_KEY='your-key' && python langgraph_demo.py")
        sys.exit(1)

    # Initialize Ray (disable dashboard to avoid frontend build errors)
    ray.init(
        ignore_reinit_error=True,
        logging_level="ERROR",
        include_dashboard=False,
    )

    # Create LangGraph agent
    adapter = LangGraphAdapter(
        model="gpt-4o-mini",
        system_prompt="You are a helpful business analyst. Use the provided tools to get data.",
    )
    session = AgentSession.remote(session_id="demo", adapter=adapter)

    print("\n=== LangGraph + Ray Agentic Demo ===\n")

    # Interaction 1
    print("User: Analyze Q4 for North America")
    print()

    result1 = ray.get(
        session.run.remote(
            "Get Q4 sales data for North America, customer feedback (positive), and SaaS market trends.",
            tools=[analyze_sales_data, process_customer_feedback, fetch_market_trends],
        )
    )

    print(f"Agent: {result1['content']}\n")

    # Interaction 2
    print("User: Forecast Q1-Q2 2025")
    print()

    result2 = ray.get(
        session.run.remote(
            "Generate a 2-quarter revenue forecast.",
            tools=[generate_forecast],
        )
    )

    print(f"Agent: {result2['content']}\n")

    history = ray.get(session.get_history.remote())
    print(f"=== Conversation: {len(history)} messages ===\n")

    ray.shutdown()


if __name__ == "__main__":
    main()
