import string

testFile = open('test1', 'r')

num_each = 1000
count_each = 0
fileNum = 0
unsortedFileName = './unsorted/unsorted' + str(fileNum).zfill(3) + '.txt'
unsortedFile = open(unsortedFileName, 'w')
data = []

while True:
    line = testFile.readline()
    data.append(line)
    if not line: break

for line in data:
    if count_each >= num_each:
        fileNum += 1
        unsortedFileName = './unsorted/unsorted' + str(fileNum).zfill(3) + '.txt'
        print(unsortedFileName)
        unsortedFile.close()
        unsortedFile = open(unsortedFileName, 'w')
        count_each = 0

    unsortedFile.write(line)
    count_each += 1

sortedData = sorted(data)

fileNum = 0
for line in sortedData:
    if count_each >= num_each:
        fileNum += 1
        unsortedFileName = './sorted/sorted' + str(fileNum).zfill(3) + '.txt'
        print(unsortedFileName)
        unsortedFile.close()
        unsortedFile = open(unsortedFileName, 'w')
        count_each = 0

    unsortedFile.write(line)
    count_each += 1


unsortedFile.close()
testFile.close()