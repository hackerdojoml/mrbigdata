if [ "$#" -eq 0 ]; then
	echo "Usage: pullhw.sh <username>"
	exit 1
fi
git fetch $1
git merge $1/master
