from itertools import starmap

def main():
    datagen = (i for i in range(10000))
    for a in starmap(lambda x,y: [y*x], map(lambda x:[x,1+x], datagen)):
        print("result= " + str(a))