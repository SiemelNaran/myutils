#!/bin/bash

# Find tokens (or words) in a file starting with or containing text.
# A word is defined as a sequence of alphanumeric characters plus underscore.

function showUsage()
{
    echo ""
    echo Usage: findTokens.sh [-h] [-a] [-r] token \[filename\]
    echo "   startOfToken : find all tokens (or words) starting with or containing this string"
    echo "   -h : show help and exit"
    echo "   -a : find tokens containing the string, otherwise find tokens starting with the string"
    echo "   -i : case insensitive"
    echo "   -r : search recursively, only works if filename is supplied"
    echo "   -f : show filenames -- will show the same token many times if in different files"
    echo "    token    : the token to search for"
    echo "    filename : the files to search, can be \*, blank means standard input"
    echo ""
}

SHOWALL=0
GREPARGS=
NOCASE=0
SHOWFILENAME=0

while getopts "hairf" opt; do
    case $opt in
     h)
         showUsage
         exit 0
         ;;
     a)
         SHOWALL=1
         ;;
     i)
         NOCASE=1
         GREPARGS="$GREPARGS -i"
         ;;
     r)
         GREPARGS="$GREPARGS -r"
         ;;
     f)
         SHOWFILENAME=1
         ;;
     \?)
         showUsage >&2
         exit 1
         ;;
  esac
done
shift $((OPTIND-1))

TOKEN=$1
shift 1

if [ -z "$TOKEN" ]; then
    echo token is required
    showUsage >&2
    exit 1
fi

GREPTEXT=""
if [ $SHOWALL -eq 0 ]; then
    GREPTEXT="\b$TOKEN"
else
    GREPTEXT="$TOKEN"
fi

if [ $NOCASE -eq 1 ]; then
    TOKEN=${TOKEN,,}
fi

set -o pipefail

grep $GREPARGS $GREPTEXT $* | while read -r fullLine; do
    if [ $SHOWFILENAME -eq 1 ]; then
        filename=`echo "$fullLine" | cut -d : -f 1`
        filename="$filename:"
    else
        filename=
    fi
    line=`echo "$fullLine" | cut -d : -f 2 | sed "s/[^A-Za-z0-9_]/ /g"`
    for word in $line; do
        if [ $NOCASE -eq 1 ]; then
            modword=${word,,}
        else
            modword=${word}
        fi
        if [ $SHOWALL -eq 0 ]; then
            if [[ "$modword" = $TOKEN* ]]; then
                echo $filename$word
            fi
        else
            if [[ "$modword" = *$TOKEN* ]]; then
                echo $filename$word
            fi
        fi
    done
done | sort -u

exit $?
