# Oracle-to-Data Lake Sync Monitor

A production-ready monitoring solution that detects and diagnoses synchronization issues between Oracle databases and data lakes (Hive/Trino). Built with Flask, SQLite, and AI-powered triage capabilities.

## 🎯 Problem Statement

Enterprises load critical data from Oracle (system of record) into data lakes for analytics and reporting. When synchronization fails:
- Lake data becomes stale (hours/days behind)
- Missing partitions cause incomplete reports
- Data quality issues propagate downstream
- Incidents are discovered by angry users, not proactive monitoring

This creates regulatory, audit, and P&L risk for organizations.

## ✨ Features

- **Real-time Sync Monitoring**: Detects gaps between Oracle source and data lake
- **Comprehensive Metrics**: 
  - Sync gap percentage
  - Data freshness lag (hours)
  - Missing partitions detection
  - Data quality scoring
  - SLA compliance tracking
- **AI-Powered Triage**: Integrates with Claude API for intelligent root cause analysis
- **Web Dashboard**: Interactive visualizations with auto-refresh
- **RESTful APIs**: JSON endpoints for integration with other systems
- **Audit Trail**: Complete history of all monitoring runs

## 🏗️ Architecture

```
oracle-lake-sync-monitor/
├── app/
│   ├── app.py                     # Flask application
│   ├── seed.py                    # Database seeder
│   ├── metrics.py                 # Metrics calculation engine
│   ├── ai_triage.py              # AI analysis integration
│   ├── connectors/
│   │   ├── oracle_source.py      # Oracle connector (real/mock)
│   │   └── hive_simulator.py     # SQLite as Hive simulator
│   ├── data_problems/
│   │   └── volume_issues.py      # Data problem injection
│   ├── models/
│   │   └── schema.sql            # Database schema
│   ├── static/                   # CSS/JS assets
│   └── templates/
│       └── dashboard.html        # Web interface
├── notebooks/
│   └── 01_explore_problem.ipynb  # Development notebook
├── data/
│   └── demo_hive.db              # SQLite database
├── tests/                        # Test suite
└── requirements.txt
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Oracle database access (optional - can run in demo mode)
- Claude API key (optional - has fallback mode)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/oracle-lake-sync-monitor.git
cd oracle-lake-sync-monitor

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the root directory:

```bash
# Oracle connection (optional)
USE_REAL_ORACLE=false
ORACLE_USER=your_user
ORACLE_PASSWORD=your_password
ORACLE_DSN=your_dsn

# Monitoring settings
ORACLE_TABLE=HR.EMPLOYEES
ORACLE_MOCK_COUNT=107
SLA_WARNING_GAP_PCT=1.0
SLA_CRITICAL_GAP_PCT=5.0

# AI configuration (optional)
USE_CLAUDE_API=true
CLAUDE_API_KEY=your_api_key
```

### Running the Application

1. **Initialize the database**:
```bash
python app/seed.py
```

2. **Create monitoring issues** (for demo):
```bash
python app/data_problems/volume_issues.py
```

3. **Start the web server**:
```bash
python app/app.py
```

4. **Access the dashboard**: http://localhost:5000

## 📊 Dashboard Features

The monitoring dashboard provides:

- **KPI Cards**: Real-time sync gap, freshness lag, data quality score, and SLA status
- **AI Triage Panel**: Intelligent analysis of root causes and recommended actions
- **Trend Visualization**: Historical sync performance (Chart.js)
- **Issue Breakdown**: Detailed view of data quality problems
- **Runbook SQL**: Generated queries for incremental sync

## 🔌 API Endpoints

```bash
GET /                    # Web dashboard
GET /api/metrics         # Current metrics (JSON)
GET /api/recommendations # AI-generated recommendations (JSON)
```

Example response:
```json
{
  "oracle_count": 107,
  "lake_count": 91,
  "sync_gap": 16,
  "gap_percentage": 14.95,
  "freshness_lag_hours": 13.5,
  "severity": "CRITICAL",
  "missing_partitions": ["2024-01-15", "2024-01-16"]
}
```

## 🧪 Development

### Running Tests
```bash
pytest tests/
```

### Using Notebooks
The project includes Jupyter notebooks for exploration:
```bash
jupyter notebook notebooks/01_explore_problem.ipynb
```

### Adding New Metrics
1. Extend `metrics.py` with new calculation functions
2. Update `dashboard.html` to display new metrics
3. Add corresponding tests in `tests/`

## 🏭 Production Deployment

### Docker Support
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "app/app.py"]
```

### Environment Variables
- `PORT`: Server port (default: 5000)
- `FLASK_ENV`: Set to 'production' for production deployment
- See `.env.example` for all configuration options

## 🔮 Roadmap

- [ ] Real Hive/Trino connector implementation
- [ ] Oracle GoldenGate CDC monitoring
- [ ] Fusion Cloud extract monitoring
- [ ] Multi-table monitoring support
- [ ] Slack/Teams notifications
- [ ] Kubernetes deployment manifests
- [ ] Grafana dashboard integration

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details

## 🙏 Acknowledgments

Built as a portfolio project demonstrating enterprise data monitoring patterns. Special thanks to the open source community for the excellent libraries used in this project.

---

**Note**: This is a demonstration project. For production use, replace the SQLite simulator with actual Hive/Trino connections and implement proper security measures.
