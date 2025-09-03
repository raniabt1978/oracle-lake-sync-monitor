# app/app.py
from flask import Flask, render_template, jsonify
from datetime import datetime
import json
import os
from dotenv import load_dotenv

from metrics import SyncMetrics
from ai_triage import AITriageEngine, demo_triage

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Initialize metrics and AI engines
metrics_engine = SyncMetrics()

@app.route('/')
def dashboard():
    """Main dashboard view"""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    """API endpoint for metrics data"""
    try:
        metrics = metrics_engine.get_all_metrics()
        return jsonify({
            'success': True,
            'data': metrics,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/triage')
def get_triage():
    """API endpoint for AI triage analysis"""
    try:
        # Get current metrics
        metrics = metrics_engine.get_all_metrics()
        
        # Run AI analysis
        if os.getenv('ANTHROPIC_API_KEY'):
            ai_engine = AITriageEngine()
            analysis = ai_engine.analyze_metrics(metrics)
        else:
            # Use demo mode if no API key
            analysis = demo_triage()
        
        return jsonify({
            'success': True,
            'data': analysis,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/chart-data')
def get_chart_data():
    """Prepare data for charts"""
    try:
        metrics = metrics_engine.get_all_metrics()
        
        # Prepare data for different charts
        chart_data = {
            'syncGauge': {
                'oracle': metrics['sync_gap']['oracle_count'],
                'hive': metrics['sync_gap']['hive_count'],
                'gapPercent': metrics['sync_gap']['gap_percent']
            },
            'issueBreakdown': {
                'labels': ['Sync Gap', 'Missing Partitions', 'Duplicates', 'Stuck Partitions'],
                'values': [
                    metrics['sync_gap']['gap_count'],
                    metrics['missing_partitions']['missing_count'],
                    metrics['duplicates']['duplicate_count'],
                    metrics['stuck_partitions']['stuck_count']
                ]
            },
            'severityDistribution': {
                'labels': ['Critical', 'Warning', 'Minor', 'OK'],
                'values': [0, 0, 0, 0]
            },
            'trend': generate_trend_data()  # Mock trend data
        }
        
        # Count severities
        for metric_group in ['sync_gap', 'missing_partitions', 'duplicates', 'stuck_partitions']:
            severity = metrics.get(metric_group, {}).get('severity', 'OK')
            if severity == 'CRITICAL':
                chart_data['severityDistribution']['values'][0] += 1
            elif severity == 'WARNING':
                chart_data['severityDistribution']['values'][1] += 1
            elif severity == 'MINOR':
                chart_data['severityDistribution']['values'][2] += 1
            else:
                chart_data['severityDistribution']['values'][3] += 1
        
        return jsonify({
            'success': True,
            'data': chart_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def generate_trend_data():
    """Generate mock historical trend data"""
    # In production, this would query historical metrics
    import random
    from datetime import timedelta
    
    now = datetime.now()
    trend_data = {
        'labels': [],
        'syncGap': [],
        'duplicates': [],
        'freshness': []
    }
    
    for i in range(7, -1, -1):
        date = (now - timedelta(days=i)).strftime('%Y-%m-%d')
        trend_data['labels'].append(date)
        
        # Simulate improving metrics
        base_gap = 15 if i > 4 else 11.21
        trend_data['syncGap'].append(base_gap + random.uniform(-2, 2))
        trend_data['duplicates'].append(10 if i > 2 else 10)
        trend_data['freshness'].append(max(1, 7 - i))
    
    return trend_data

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    print("🚀 Starting Oracle-Hive Sync Monitor")
    print("📊 Dashboard: http://localhost:5001")
    print("🤖 AI Triage: " + ("Enabled" if os.getenv('ANTHROPIC_API_KEY') else "Demo Mode"))
    
    app.run(debug=True, port=5001)