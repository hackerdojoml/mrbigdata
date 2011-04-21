if [ -f student.init ];
then
	git fetch upstream
	git merge upstream/master
	exit 0
fi
