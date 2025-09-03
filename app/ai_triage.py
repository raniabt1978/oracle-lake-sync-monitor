# app/ai_triage.py
import os
import json
from typing import Dict, List, Any
from datetime import datetime
import anthropic
from metrics import SyncMetrics
from dotenv import load_dotenv

# Load .env file
load_dotenv()

class AITriageEngine:
    """AI-powered triage for data sync issues using Claude Haiku"""
    
    def __init__(self, api_key: str = None):
        """
        Initialize AI triage engine
        
        Args:
            api_key: Anthropic API key (or set ANTHROPIC_API_KEY env var)
        """
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("Anthropic API key required. Set ANTHROPIC_API_KEY environment variable.")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = "claude-3-haiku-20240307"  # Fast and cost-effective
        
    def analyze_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze metrics and provide recommendations
        
        Args:
            metrics: Dictionary of metrics from SyncMetrics.get_all_metrics()
            
        Returns:
            Analysis with root cause and recommendations
        """
        # Build context for Claude
        prompt = self._build_analysis_prompt(metrics)
        
        try:
            # Call Claude Haiku
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                temperature=0.3,  # Lower temperature for consistent analysis
                messages=[{
                    "role": "user", 
                    "content": prompt
                }]
            )
            
            # Parse response
            analysis_text = response.content[0].text
            
            # Structure the response
            return self._parse_analysis(analysis_text, metrics)
            
        except Exception as e:
            return {
                "error": f"AI analysis failed: {str(e)}",
                "fallback_recommendations": self._get_fallback_recommendations(metrics)
            }
    
    def _build_analysis_prompt(self, metrics: Dict[str, Any]) -> str:
        """Build prompt for Claude with metrics context"""
        
        sync_gap = metrics.get('sync_gap', {})
        missing_parts = metrics.get('missing_partitions', {})
        freshness = metrics.get('data_freshness', {})
        duplicates = metrics.get('duplicates', {})
        stuck = metrics.get('stuck_partitions', {})
        
        prompt = f"""You are a data engineering expert analyzing Oracle to Hive sync issues.

Current Metrics:
- Sync Gap: {sync_gap.get('gap_percent', 0)}% ({sync_gap.get('gap_count', 0)} records missing)
- Missing Partitions: {missing_parts.get('missing_count', 0)} partitions
- Data Freshness: {freshness.get('data_lag_days', 0)} days behind
- Duplicates: {duplicates.get('duplicate_count', 0)} found
- Stuck Partitions: {stuck.get('stuck_count', 0)} partitions

Analyze these metrics and provide:
1. ROOT CAUSE: What's likely causing these issues? (one sentence)
2. SEVERITY: Critical/High/Medium/Low based on business impact
3. RECOMMENDATIONS: List 3 specific actions to fix the issues (be concise)
4. PRIORITY: Which issue to fix first and why (one sentence)

Format your response exactly as:
ROOT CAUSE: [your analysis]
SEVERITY: [level]
RECOMMENDATIONS:
- [action 1]
- [action 2]
- [action 3]
PRIORITY: [which issue first and why]"""
        
        return prompt
    
    def _parse_analysis(self, analysis_text: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Claude's response into structured format"""
        
        lines = analysis_text.strip().split('\n')
        
        # Extract components
        root_cause = ""
        severity = "UNKNOWN"
        recommendations = []
        priority = ""
        
        current_section = None
        
        for line in lines:
            line = line.strip()
            
            if line.startswith("ROOT CAUSE:"):
                root_cause = line.replace("ROOT CAUSE:", "").strip()
            elif line.startswith("SEVERITY:"):
                severity = line.replace("SEVERITY:", "").strip()
            elif line.startswith("RECOMMENDATIONS:"):
                current_section = "recommendations"
            elif line.startswith("PRIORITY:"):
                priority = line.replace("PRIORITY:", "").strip()
                current_section = None
            elif current_section == "recommendations" and line.startswith("-"):
                recommendations.append(line[1:].strip())
        
        # Calculate risk score
        risk_score = self._calculate_risk_score(metrics)
        
        return {
            "analysis_timestamp": datetime.now().isoformat(),
            "model_used": self.model,
            "root_cause": root_cause,
            "severity": severity,
            "risk_score": risk_score,
            "recommendations": recommendations,
            "priority_action": priority,
            "metrics_summary": self._summarize_metrics(metrics),
            "estimated_fix_time": self._estimate_fix_time(metrics)
        }
    
    def _calculate_risk_score(self, metrics: Dict[str, Any]) -> int:
        """Calculate overall risk score (0-100)"""
        score = 0
        
        # Sync gap contributes most to risk
        gap_percent = metrics.get('sync_gap', {}).get('gap_percent', 0)
        score += min(gap_percent * 3, 40)  # Max 40 points
        
        # Missing partitions
        missing_count = metrics.get('missing_partitions', {}).get('missing_count', 0)
        score += min(missing_count * 10, 20)  # Max 20 points
        
        # Data freshness
        lag_days = metrics.get('data_freshness', {}).get('data_lag_days', 0)
        score += min(lag_days * 2, 20)  # Max 20 points
        
        # Duplicates
        dup_count = metrics.get('duplicates', {}).get('duplicate_count', 0)
        score += min(dup_count * 2, 10)  # Max 10 points
        
        # Stuck partitions
        stuck_count = metrics.get('stuck_partitions', {}).get('stuck_count', 0)
        score += min(stuck_count * 5, 10)  # Max 10 points
        
        return min(int(score), 100)
    
    def _summarize_metrics(self, metrics: Dict[str, Any]) -> Dict[str, str]:
        """Create human-readable summary of metrics"""
        sync_gap = metrics.get('sync_gap', {})
        
        return {
            "sync_status": f"{sync_gap.get('gap_percent', 0)}% behind Oracle",
            "data_quality": f"{metrics.get('duplicates', {}).get('duplicate_count', 0)} duplicates",
            "completeness": f"{metrics.get('missing_partitions', {}).get('missing_count', 0)} missing partitions",
            "freshness": f"{metrics.get('data_freshness', {}).get('data_lag_days', 0)} days lag"
        }
    
    def _estimate_fix_time(self, metrics: Dict[str, Any]) -> str:
        """Estimate time to fix all issues"""
        # Simple estimation based on issue counts
        total_issues = (
            metrics.get('sync_gap', {}).get('gap_count', 0) +
            metrics.get('missing_partitions', {}).get('missing_count', 0) * 100 +
            metrics.get('duplicates', {}).get('duplicate_count', 0) * 10
        )
        
        if total_issues < 100:
            return "< 1 hour"
        elif total_issues < 1000:
            return "1-4 hours"
        elif total_issues < 5000:
            return "4-8 hours"
        else:
            return "1-2 days"
    
    def _get_fallback_recommendations(self, metrics: Dict[str, Any]) -> List[str]:
        """Fallback recommendations if AI fails"""
        recommendations = []
        
        # Check sync gap
        if metrics.get('sync_gap', {}).get('gap_percent', 0) > 10:
            recommendations.append("Run immediate full sync from Oracle to Hive")
        
        # Check missing partitions
        if metrics.get('missing_partitions', {}).get('missing_count', 0) > 0:
            recommendations.append("Backfill missing partitions using partition recovery job")
        
        # Check duplicates
        if metrics.get('duplicates', {}).get('duplicate_count', 0) > 0:
            recommendations.append("Run deduplication script on affected tables")
        
        # Check freshness
        if metrics.get('data_freshness', {}).get('data_lag_days', 0) > 7:
            recommendations.append("Check ETL pipeline status and logs for failures")
        
        return recommendations if recommendations else ["Review all metrics manually"]

# Demo function that works without API key
def demo_triage():
    """Demo triage analysis without requiring API key"""
    print("🤖 AI Triage Demo (Simulated)\n")
    
    # Get real metrics
    metrics_engine = SyncMetrics()
    metrics = metrics_engine.get_all_metrics()
    
    # Simulate AI analysis
    risk_score = 0
    if metrics['sync_gap']['gap_percent'] > 10:
        risk_score += 40
    if metrics['missing_partitions']['missing_count'] > 0:
        risk_score += 20
    if metrics['duplicates']['duplicate_count'] > 0:
        risk_score += 15
    
    analysis = {
        "analysis_timestamp": datetime.now().isoformat(),
        "model_used": "claude-3-haiku (simulated)",
        "root_cause": "ETL pipeline experiencing intermittent failures causing sync lag and partition drops",
        "severity": "HIGH" if risk_score > 50 else "MEDIUM",
        "risk_score": risk_score,
        "recommendations": [
            f"Immediate action: Resync {metrics['sync_gap']['gap_count']} missing records from Oracle",
            f"Backfill {metrics['missing_partitions']['missing_count']} missing partitions from source",
            f"Remove {metrics['duplicates']['duplicate_count']} duplicate records using dedup script"
        ],
        "priority_action": "Fix sync gap first as it affects downstream reporting accuracy",
        "metrics_summary": {
            "sync_status": f"{metrics['sync_gap']['gap_percent']}% behind Oracle",
            "data_quality": f"{metrics['duplicates']['duplicate_count']} duplicates",
            "completeness": f"{metrics['missing_partitions']['missing_count']} missing partitions",
            "freshness": f"{metrics['data_freshness']['data_lag_days']} days lag"
        },
        "estimated_fix_time": "2-4 hours"
    }
    
    print(json.dumps(analysis, indent=2))
    return analysis

# Test with real API
def test_with_api():
    """Test with real Anthropic API"""
    try:
        # Get metrics
        metrics_engine = SyncMetrics()
        metrics = metrics_engine.get_all_metrics()
        
        # Run AI analysis
        triage = AITriageEngine()
        analysis = triage.analyze_metrics(metrics)
        
        print("🤖 AI Triage Analysis\n")
        print(json.dumps(analysis, indent=2))
        
    except ValueError as e:
        print(f"⚠️  {e}")
        print("\nRunning demo mode instead...")
        demo_triage()

if __name__ == "__main__":
    # Run demo if no API key, otherwise use real API
    if os.getenv('ANTHROPIC_API_KEY'):
        test_with_api()
    else:
        demo_triage()