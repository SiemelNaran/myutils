#!/bin/bash

# find TODO in folders
# the format of a TODO should be
#     TODO: category: subcategory: optional description text

function showUsage()
{
    echo ""
    echo Usage: findTodo.sh [-h] -c category [-s subcategory] [-e extension]* [folders]
    echo "   Find TODO lines in folders for a specified category."
    echo "   The result will be sorted by subcategory."
    echo "      -h : show help and exit"
    echo "      -c category : the category to search for, use . for all categories"
    echo "      -s subcategory: the subcategory to search for"
    echo "      -e extension: only search files with this extension for example -e java, may be repeated"
    echo "      -v verbose: if present turn on verbose mode"
    echo "      folders: the folders to search for space separated, missing means the current folder"
    echo ""
}

CATEGORY=
SUBCATEGORY=
EXTENSION_ARGS=
VERBOSE=0
SEARCH_FOLDERS=.

while getopts "hc:s:e:v" opt; do
    case $opt in
     h)
         showUsage
         exit 0
         ;;
     c)
         CATEGORY="$OPTARG"
         ;;
     s)
         SUBCATEGORY="$OPTARG"
         ;;
     e)
         EXTENSION_ARGS="$EXTENSION_ARGS --include *.$OPTARG"
         ;;
     v)
         VERBOSE=1
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
elif [ "$CATEGORY" = "." ]; then
    CATEGORY="\w+"
    CATEGORY1="\\\w+"
else
    CATEGORY1="$CATEGORY"
fi

if [ -z "$SUBCATEGORY" ]; then
    SUBCATEGORY="\w+"
    SUBCATEGORY1="\\\w+"
else
    SUBCATEGORY1="$SUBCATEGORY"
fi

if [ -z "$SEARCH_FOLDERS" ]; then
    SEARCH_FOLDERS=.
fi

# GREPTEXT is the text to search for using grep
# REGEX is a regular expression with capturing group 1 as the category, and capturing group 2 as the sub-category
GREPTEXT="TODO: $CATEGORY: $SUBCATEGORY"
REGEX=".*TODO: ($CATEGORY1): ($SUBCATEGORY1).*"

if [ $VERBOSE -ne 0 ]; then
    echo SEARCH_FOLDERS=$SEARCH_FOLDERS
fi

set -o pipefail

if [ $VERBOSE -ne 0 ]; then
   set -x
fi
grep -EHnr $EXTENSION_ARGS "$GREPTEXT" $SEARCH_FOLDERS | gawk -v REGEX="$REGEX" '{ print gensub(REGEX, "\\1.\\2", "1") " @ " $0 }'
EXITCODE=$?
set +x

exit $EXITCODE
