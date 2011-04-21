if [ -f student.init ];
then
	echo "Do not run this command as a student"
	exit 1
fi

git remote debnathsinha git://github.com/debnathsinha/mrbigdata.git
