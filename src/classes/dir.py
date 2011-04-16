import os
for i in range(1,11):
		s = "class" + str(i)
		if not os.path.exists(s):
			os.mkdir(s)
			t = "echo \'This dir is for src for class" + str(i) + "\' >> " + s + "/README"
			os.system(t)
