# Contributing to DeanFi Collectors

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## 🚀 Quick Start for Contributors

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/deanfi-collectors.git
cd deanfi-collectors
```

### 2. Set Up Development Environment

```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Edit .env and add your API keys
# Get free Finnhub key: https://finnhub.io/register
```

### 3. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

### 4. Make Your Changes

- Follow existing code patterns
- Add documentation for new features
- Test locally before committing

### 5. Test Your Changes

```bash
# Run a specific collector to verify it works
cd dailynews
python fetch_top_news.py

# Check the output
cat top_news.json | head -50
```

### 6. Commit with Conventional Commits

We use [Conventional Commits](https://www.conventionalcommits.org/) for clear history:

```bash
# Format: type(scope): description
git commit -m "feat(earnings): add earnings surprise collector"
git commit -m "fix(news): handle rate limit errors gracefully"
git commit -m "docs(readme): add setup instructions"
```

**Types:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation only
- `refactor:` - Code restructuring (no functional changes)
- `test:` - Adding tests
- `chore:` - Maintenance (dependencies, configs)

### 7. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then go to GitHub and create a Pull Request from your fork.

## 📋 Pull Request Guidelines

### PR Title Format
Use conventional commit format:
```
feat(collector): add economic calendar data source
fix(cache): resolve cache invalidation bug
docs: update API documentation
```

### PR Description Template

```markdown
## Description
Brief description of what this PR does and why.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Data Source (if applicable)
- [ ] Uses existing data sources (Finnhub, Yahoo Finance)
- [ ] Adds new data source: _________________
  - API documentation: _________________
  - Free tier limits: _________________
  - Rate limit handling: Yes / No

## Testing
- [ ] Tested locally with valid API key
- [ ] Output JSON validated (provide sample below)
- [ ] No hardcoded secrets or API keys
- [ ] Follows existing code patterns
- [ ] Documentation updated

## Sample Output
```json
{
  "example": "paste sample JSON output here"
}
```

## Checklist
- [ ] Code follows PEP 8 style guidelines
- [ ] Added/updated docstrings
- [ ] Updated README if needed
- [ ] Tested with rate limiting
- [ ] Error handling implemented
```

## 🎯 What to Contribute

### Good First Issues
- Add new data collectors (economic indicators, options data, etc.)
- Improve error handling in existing collectors
- Add data validation
- Improve documentation
- Add examples and tutorials

### Ideas We'd Love to See
- **New Data Sources:**
  - Economic calendar (Fed meetings, GDP releases)
  - Options flow and unusual activity
  - Institutional holdings changes
  - Dark pool activity
  - Crypto market data
  
- **Infrastructure:**
  - Unit tests for collectors
  - Data quality validation
  - Performance monitoring
  - Retry logic for API failures
  
- **Documentation:**
  - Video tutorials
  - Jupyter notebook examples
  - API rate limit optimization guides

## 🔒 Security Guidelines

**CRITICAL - Never commit secrets!**

❌ **DON'T:**
- Hardcode API keys in code or config files
- Commit `.env` files
- Include credentials in examples
- Paste API responses with sensitive data

✅ **DO:**
- Use environment variables via `os.getenv()`
- Provide `.env.example` templates
- Use placeholders in documentation
- Review diffs before committing

## 📐 Code Style Guidelines

### Python Style
- Follow [PEP 8](https://peps.python.org/pep-0008/)
- Use 4 spaces for indentation
- Maximum line length: 100 characters (flexible for readability)
- Use descriptive variable names

### Docstring Format
```python
def fetch_data(ticker: str, start_date: str, end_date: str) -> dict:
    """
    Fetch market data for a specific ticker and date range.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL')
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        Dictionary containing market data with keys:
        - 'data': List of price records
        - 'metadata': Information about the request
        
    Raises:
        ValueError: If date format is invalid
        requests.HTTPError: If API request fails
    """
```

### Configuration Files
- Use YAML for configuration
- Include comments explaining each setting
- Provide sensible defaults
- Use environment variable placeholders: `"${ENV_VAR}"`

### Error Handling
```python
try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
except requests.Timeout:
    print(f"⏱️  Timeout fetching {ticker}", file=sys.stderr)
    return None
except requests.HTTPError as e:
    print(f"❌ HTTP error for {ticker}: {e}", file=sys.stderr)
    return None
```

## 🤝 Review Process

### What Happens After You Submit a PR

1. **Automated Checks** - CI runs (if configured)
2. **Maintainer Review** - Usually within 2-3 days
3. **Discussion** - We may request changes or ask questions
4. **Approval** - Once approved, maintainer will merge
5. **Credit** - You'll be listed in contributors!

### Review Criteria
- ✅ Code quality and style
- ✅ Security (no hardcoded secrets)
- ✅ Rate limit compliance
- ✅ Documentation completeness
- ✅ Error handling
- ✅ Test coverage (if applicable)

## 💬 Communication

- **Questions?** Open a [Discussion](../../discussions)
- **Bug Found?** Open an [Issue](../../issues)
- **Feature Idea?** Open an [Issue](../../issues) with `[Feature Request]`

## 📜 License

By contributing, you agree that your contributions will be licensed under the MIT License.

## 🙏 Thank You!

Every contribution, no matter how small, makes this project better. We appreciate your time and effort!
