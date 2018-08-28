#!/bin/bash

# Copy files that contain <sourceSubstring> to a similar filename where <sourceSubstring> is replaced by <dest>

function showUsage()
{
    echo ""
    echo "Usage: multifile.sh [-h] [-c] [-m] [-d directory] [-n] [-p includeFilePattern] [-x excludeFilePattern] [-f] [-v] [-r command] sourceSubstring destSubstring"

    echo "   Copy files that contain <sourceSubstring> to a similar filename where <sourceSubstring> is replaced by <dest>"
    echo "   <sourceSubstring> can only contain numbers and letters"
    echo "   The filename, as separated by other characters, must contain the whole word <sourceSubstring>"
    echo "   Default copy command is cp -iv"

    echo "   -h : show help and exit"
    echo "   -c : cp files, exactly one of -c or -m is required"
    echo "   -m : mv files, exactly one of -c or -m is required"
    echo "   -d : switch to this directory"
    echo "   -n : dry run, just show the copy commands that would be run"
    echo "   -p : only select files matching this pattern, for example -p \"^start\""
    echo "   -x : exclude files matching this pattern, for example -x \".java\""
    echo "   -f : force overwriting files"
    echo "   -v : echo cp/mv commands, and note that the call to cp/mv always uses -v"
    echo "   -r : execute this command on each new files, for example -r \"git add %s\" or -r \"git add\""

    echo "   sourceSubstring : find files containing this substring as a whole word"
    echo "   destSubstring : the string to change sourceSubstring to"

    echo ""
}

BASECMD=
DIRECTORY=.
DRYRUN=0
INCLUDEFILEPATTERN=
EXCLUDEFILEPATTERN=
FORCEARGS=-i
VERBOSEMODE=0
RUNTEMPLATE=

while getopts "hcmd:np:x:fvr:" opt; do
    case $opt in
     h)
         showUsage
         exit 0
         ;;
     c)
         if [ -n "$BASECMD" ]; then
             echo Exactly one of -c or -m is required
         fi
         BASECMD=cp
         ;;
     m)
         if [ -n "$BASECMD" ]; then
             echo Exactly one of -c or -m is required
         fi
         BASECMD=mv
         ;;
     d)
         DIRECTORY=$OPTARG
         ;;
     n)
         DRYRUN=1
         ;;
     p)
         INCLUDEFILEPATTERN=$OPTARG
         ;;
     x)
         EXCLUDEFILEPATTERN=$OPTARG
         ;;
     f)
         FORCEARGS=-f
         ;;
     v)
         VERBOSEMODE=1
         ;;
     r)
         RUNTEMPLATE=$OPTARG
         if [[ ! ( "$RUNTEMPLATE" =~ %s ) ]]; then
            RUNTEMPLATE="$RUNTEMPLATE %s"
         fi
         ;;
     \?)
         showUsage >&2
         exit 1
         ;;
  esac
done
shift $((OPTIND-1))

if [ -z "$BASECMD" ]; then
    echo Exactly one of -c or -m is required
fi

if [ $# -ne 2 ]; then
    echo "Not enough command line arguments"
    showUsage >&2
    exit 1
fi

SOURCE=$1
DEST=$2
shift 2

if [ -z "$SOURCE" -o -z "$DEST" ]; then
    echo sourceSubstring and destSubstring are required
    showUsage >&2
    exit 1
fi

function checkOnlyLettersAndDigits()
{
    if [[ ! ("$2" =~ ^[A-Za-z0-9_]*$) ]]; then
        echo $1 can only contain letters and digits
        exit 1
    fi
}

checkOnlyLettersAndDigits sourceSubstring $SOURCE
checkOnlyLettersAndDigits destSubstring $DEST

if [ "$SOURCE" == "$DEST" ]; then
    echo sourceSubstring and destSubstring cannot be the same
fi

cd "$DIRECTORY"
RESULT=$?
if [ $RESULT -ne 0 ]; then
    echo Failed to switch to directory $DIRECTORY
    exit $RESULT
fi

set -o pipefail

for file in *; do
    if [ -n "$INCLUDEFILEPATTERN" ]; then
        if [[ ! ( "$file" =~ $INCLUDEFILEPATTERN ) ]]; then
            continue
        fi
    fi
    if [ -n "$EXCLUDEFILEPATTERN" ]; then
        if [[ ( "$file" =~ $EXCLUDEFILEPATTERN ) ]]; then
            continue
        fi
    fi
    if [[ "$file" =~ (.*)$SOURCE(.*) ]]; then
        filelen=${#file}
        newfile=${BASH_REMATCH[1]}$DEST${BASH_REMATCH[2]}
        COMMAND="$BASECMD -v $FORCEARGS \"$file\" \"$newfile\""
        if [ $DRYRUN -eq -0 ]; then
            test -e "$newfile"
            newfileAlreadyExists=$?
            if [ $VERBOSEMODE -eq 1 ]; then    
                echo $COMMAND
            fi
            eval $COMMAND
            RESULT=$?
            if [ $RESULT -ne 0 -a "$FORCEARGS" == "-i" -a $newfileAlreadyExists -eq 0 ]; then
                continue
            fi
            if [ $RESULT -ne 0 ]; then
                echo "Failed to copy $file $newfile"
                exit $RESULT
            fi
            if [ -n "$RUNTEMPLATE" ]; then
                RUNCOMMAND=${RUNTEMPLATE/\%s/$newfile}
                echo $RUNCOMMAND
                eval $RUNCOMMAND
                RESULT=$?
                if [ $RESULT -ne 0 ]; then
                    echo "Failed to run $RUNCOMMAND"
                    exit $RESULT
                fi
            fi
        else
            echo $COMMAND
        fi
    fi
done

exit $?
