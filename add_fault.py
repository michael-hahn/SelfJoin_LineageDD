from random import randint
inputfile = open("/Users/Michael/IdeaProjects/SelfJoin/file1s.data", "r")
outputfile = open("/Users/Michael/IdeaProjects/SelfJoin/file1s_faults.data", "w")
line = inputfile.readline()
while line:
	fault1 = randint(1, 1000000)
	if fault1 <= 400:
		entryList = line.split(",")
		length = len(entryList)
		indexToFault = randint(0, length - 1)
		if indexToFault == length - 1:
			entryList[indexToFault] = "entryNumA" + "\n"
		else:
			entryList[indexToFault] = "entryNumA"
		newLine = entryList[0]
		for i in range(1, length):
			newLine = newLine + "," + entryList[i]
		outputfile.write(newLine)
	else:
		outputfile.write(line)
	line = inputfile.readline()
inputfile.close()
outputfile.close()