#!/bin/bash

# find TODO in folders
# the format of a TODO should be
#     TODO: category: subcategory: optional description text

function showUsage()
{
    echo ""
    echo Usage: findTodo.sh [-h] -c category [folders]
    echo "   Find TODO lines in folders for a specified category."
    echo "   The result will be sorted by subcategory."
    echo "      -h : show help and exit"
    echo "      -c category : the category to search for, use . for all categories"
    echo "      -folders: the folders to search for space separated, missing means the current folder"
    echo ""
}

CATEGORY=0
SEARCH_FOLDERS=.

while getopts "hc:" opt; do
    case $opt in
     h)
         showUsage
         exit 0
         ;;
     c)
         CATEGORY="$OPTARG"
         ;;
     \?)
         showUsage >&2
         exit 1
         ;;
  esac
done
shift $((OPTIND-1))

SEARCH_FOLDERS=$*
shift 1

if [ -z "$CATEGORY" ]; then
    echo category is required
    showUsage >&2
    exit 1
fi

if [ -z "$SEARCH_FOLDERS" ]; then
    SEARCH_FOLDERS=.
fi

echo SEARCH_FOLDERS=$SEARCH_FOLDERS

# GREPTEXT is the text to search for using grep
# REGEX is a regular expression with capturing group 1 as the category, and capturing group 2 as the sub-category

if [ "$CATEGORY" = "." ]; then
    GREPTEXT="TODO: "
    REGEX="TODO: (\\\w+): (\\\w+)"
else
    GREPTEXT="TODO: $CATEGORY"
    REGEX=".*TODO: ($CATEGORY): (\\\w+).*"
fi

set -o pipefail

grep -Hnr "$GREPTEXT" $SEARCH_FOLDERS | gawk -v REGEX="$REGEX" '{ print gensub(REGEX, "\\1.\\2", "1") " @ " $0 }'

exit $?
