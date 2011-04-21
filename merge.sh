if [ -f student.init ];
then
	git fetch upstream
	git merge upstream/master
fi
