if [ ! -f student.init ];
then
	git remote add upstream git://github.com/hackerdojoml/mrbigdata.git
	touch student.init
else
	echo "Already done initializing"
fi
