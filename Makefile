all:  ts

ts: TS.hs S.hs
	ghc --make -odir objs -hidir objs -o ts TS.hs -O3

clean:
	rm -rf ts objs/
