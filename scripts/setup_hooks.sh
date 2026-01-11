#!/bin/bash
# Setup Git hooks with lefthook

set -e

echo "🪝 Setting up Git hooks with Lefthook"
echo "====================================="
echo ""

# Check if lefthook is installed
if ! command -v lefthook &> /dev/null; then
    echo "📦 Installing lefthook..."

    # Try pip install
    if command -v pip &> /dev/null; then
        pip install lefthook
    elif command -v uv &> /dev/null; then
        uv pip install lefthook
    elif command -v brew &> /dev/null; then
        brew install lefthook
    else
        echo "❌ Error: Could not install lefthook"
        echo "Please install manually:"
        echo "  pip install lefthook"
        echo "  OR"
        echo "  brew install lefthook (macOS)"
        exit 1
    fi
fi

echo "✅ Lefthook installed: $(lefthook version)"
echo ""

# Install hooks
echo "🔧 Installing Git hooks..."
lefthook install

echo ""
echo "✅ Git hooks installed successfully!"
echo ""
echo "Hooks configured:"
echo "  ✓ Ruff linting (auto-fix)"
echo "  ✓ Ruff formatting check"
echo "  ✓ MyPy type checking"
echo "  ✓ Pytest tests"
echo ""
echo "To skip hooks for one commit:"
echo "  LEFTHOOK=0 git commit -m \"message\""
echo ""
echo "To uninstall hooks:"
echo "  lefthook uninstall"
echo ""
