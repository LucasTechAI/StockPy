#!/bin/bash
# filepath: /home/lucas/StockPy/setup/formatter_code.sh

echo "🐍 Formatting Python files with Black..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if Black is installed
if ! command -v black &> /dev/null; then
    echo -e "${RED}❌ Black is not installed!${NC}"
    echo "Install with: pip install black"
    exit 1
fi

# Project root directory
PROJECT_DIR="/home/lucas/StockPy"

echo -e "${BLUE}🔍 StockPy Code Formatter${NC}"
echo -e "${YELLOW}📁 Searching for Python files in: ${PROJECT_DIR}${NC}"

# Find all .py files excluding __pycache__ and .venv directories
PYTHON_FILES=$(find "$PROJECT_DIR" -name "*.py" -type f \
    -not -path "*/__pycache__/*" \
    -not -path "*/.venv/*" \
    -not -path "*/venv/*")

if [ -z "$PYTHON_FILES" ]; then
    echo -e "${RED}❌ No Python files found!${NC}"
    exit 1
fi

echo -e "${GREEN}📋 Python files found:${NC}"
echo "$PYTHON_FILES" | while read -r file; do
    rel_path=$(realpath --relative-to="$PROJECT_DIR" "$file")
    echo "  - $rel_path"
done

echo ""
echo -e "${YELLOW}🔧 Running Black formatter...${NC}"

# Execute Black on all Python files with project-specific settings
black \
    --line-length 88 \
    --target-version py39 \
    --include '\.pyi?$' \
    --extend-exclude '__pycache__|\.venv|venv' \
    $PYTHON_FILES

# Check for errors
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Formatting completed successfully!${NC}"
    
    # Count formatted files
    FILE_COUNT=$(echo "$PYTHON_FILES" | wc -l)
    echo -e "${GREEN}📊 Total files processed: ${FILE_COUNT}${NC}"
    
    # Show which files were reformatted (if any)
    echo -e "${BLUE}🔄 Files that were reformatted:${NC}"
    git diff --name-only 2>/dev/null || echo "  (Git not available to show changes)"
    
else
    echo -e "${RED}❌ Error during formatting!${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}📈 Formatting statistics:${NC}"
echo "$PYTHON_FILES" | while read -r file; do
    if [ -f "$file" ]; then
        lines=$(wc -l < "$file")
        rel_path=$(realpath --relative-to="$PROJECT_DIR" "$file")
        echo "  - $rel_path: $lines lines"
    fi
done

echo ""
echo -e "${GREEN}🎉 StockPy code formatting completed!${NC}"
echo -e "${BLUE}💡 Run 'git diff' to see the changes made${NC}"