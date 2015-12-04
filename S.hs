{-# LANGUAGE ForeignFunctionInterface, PatternGuards #-}
{-# OPTIONS_GHC -fno-warn-tabs #-}

module S where

import Control.Exception (bracket)

import Control.Monad
import Control.Monad.State

import Control.Concurrent
import Control.Concurrent.MVar

import Data.Bits

import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Builder as BSB

import Data.Int

import Data.IORef

import qualified Data.List as List

import qualified Data.Map as Map

import qualified Data.Maybe as Maybe

import Data.Monoid

import qualified Data.Set as Set

import Data.Word

import System.IO

import Text.Printf

-------------------------------------------------------------------------------
-- Constants.

defaultPageBits :: Int
defaultPageBits = 13

defaultMemoryPartSize :: WideInt
defaultMemoryPartSize = 256*1024

minSizeIncrement :: WideInt
minSizeIncrement = defaultMemoryPartSize*2

-------------------------------------------------------------------------------
-- Data types.

type WideInt = Int64

type BlockCounts = Map.Map WideInt WideInt

data LSMInfo = LSMInfo {
	  lsmiHeader		:: LSMHeader
	, lsmiStates		:: [LSMState]
	}
	deriving Show

emptyLSMInfo :: Int -> WideInt -> LSMInfo
emptyLSMInfo pageBits statesCount = LSMInfo h (map emptyLSMState [0 .. lsmhStatesCount h-1])
	where
		h = defaultLSMHeader pageBits statesCount

_checkLength :: WideInt -> WideInt -> WideInt
_checkLength a b
	| a == b = a
	| otherwise = error $ "different lengths for same address: a "++show a++" and b "++show b

_allAddrs :: (b -> Map.Map WideInt WideInt) -> (a -> [b]) -> a -> Map.Map WideInt WideInt
_allAddrs g f = Map.unionsWith _checkLength . map g . f

lsmInfoAddrs :: LSMInfo -> Map.Map WideInt WideInt
lsmInfoAddrs = _allAddrs lsmStateAddrs lsmiStates

lsmStateAddrs :: LSMState -> Map.Map WideInt WideInt
lsmStateAddrs = _allAddrs lsmLevelAddrs lsmsLevels

lsmLevelAddrs :: LSMLevel -> Map.Map WideInt WideInt
lsmLevelAddrs = _allAddrs lsmRunAddrs lsmlRuns

lsmRunAddrs = _allAddrs (\(LSMBlock a l) -> Map.singleton a l) lsmrBlocks

lsmInfoPageSize :: LSMInfo -> WideInt
lsmInfoPageSize = lsmhPageSize . lsmiHeader

lsmPagesBytes :: LSM -> WideInt -> WideInt
lsmPagesBytes lsm pages = shiftL pages (lsmhPageShift $ lsmHeader lsm)

lsmBytesPages :: LSM -> WideInt -> WideInt
lsmBytesPages lsm bytes = shiftR (bytes+shiftL 1 shift - 1) shift
	where
		shift = lsmhPageShift $ lsmHeader lsm

lsmStateRecPages :: LSM -> WideInt
lsmStateRecPages lsm = lsmhStateRecPages (lsmHeader lsm)

lsmStatePage :: LSM -> WideInt -> WideInt
lsmStatePage lsm physIndex = 1 + lsmStateRecPages lsm * physIndex

lsmHeader :: LSM -> LSMHeader
lsmHeader = lsmiHeader . lsmInfo

lsmInfoHeaderPages :: LSMInfo -> WideInt
lsmInfoHeaderPages lsmi = lsmhStatesCount h * (lsmhStateRecSize h + ps `div` ps) + 1
	where
		ps = lsmhPageSize h
		h = lsmiHeader lsmi

defaultLSMHeader :: Int -> WideInt -> LSMHeader
defaultLSMHeader pageBits statesCount
	| pageBits < 6 = error "page size less than 64 is not supported."
	| otherwise = LSMHeader 0 pageBits (shiftL 1 pageBits) statesCount defRecSize (shiftR defRecSize pageBits)
	where
		defRecSize = 32768

data LSMHeader = LSMHeader {
	  lsmhVersion		:: WideInt
	, lsmhPageShift		:: Int
	, lsmhPageSize		:: WideInt
	, lsmhStatesCount	:: WideInt
	, lsmhStateRecSize	:: WideInt
	, lsmhStateRecPages	:: WideInt
	}
	deriving Show

data LSMState = LSMState {
	  lsmsPhysIndex		:: WideInt
	, lsmsSeqIndex		:: WideInt
	, lsmsLevels		:: [LSMLevel]
	}
	deriving (Eq, Ord, Show)

emptyLSMState :: WideInt -> LSMState
emptyLSMState i = LSMState i i []

data LSMLevel = LSMLevel {
	  lsmlPairsCount	:: WideInt
	, lsmlKeysSize		:: WideInt
	, lsmlDataSize		:: WideInt
	, lsmlRuns		:: [LSMRun]
	}
	deriving (Eq, Ord, Show)

data LSMRun = LSMRun { lsmrBlocks :: [LSMBlock]}
	deriving (Eq, Ord, Show)

data LSMBlock = LSMBlock {
	  lsmbAddr
	, lsmbSize		:: WideInt
	}
	deriving (Eq, Ord, Show)

type LSMCmdChan = Chan Command

data LSM = LSM {
	  lsmInfo		:: LSMInfo
	, lsmCmdChan		:: LSMCmdChan
	, lsmBlockCounts	:: BlockCounts
	, lsmFreeBlocks		:: [LSMBlock]
	, lsmPhysPagesStart	:: WideInt
	, lsmHandle		:: Handle
	, lsmBranching		:: WideInt
	, lsmPagePreread	:: WideInt
	}

findRecentLevels :: LSM -> [LSMLevel]
findRecentLevels lsm = lsmsLevels $ findMostRecent $ lsmiStates $lsmInfo lsm
	where
		findMostRecent ss = head $ filter ((==mx) . lsmsSeqIndex) ss
			where
				mx = maximum $ map lsmsSeqIndex ss

lsmLevelsBlocks :: [LSMLevel] -> [LSMBlock]
lsmLevelsBlocks = concatMap lsmrBlocks . concatMap lsmlRuns

-------------------------------------------------------------------------------
-- The builder with length.

data Builder = Builder {builderLength :: !Int, builderBuilder :: BSB.Builder }

instance Show Builder where
	show b = "lazyByteString "++show (toLazyByteString b)

builderNull :: Builder -> Bool
builderNull = (<1) . builderLength

instance Monoid Builder where
	mappend (Builder l1 b1) (Builder l2 b2) = Builder (l1+l2) (mappend b1 b2)
	mempty = Builder 0 mempty

toLazyByteString :: Builder -> BS.ByteString
toLazyByteString (Builder _ b) = BSB.toLazyByteString b

hPutBuilder :: Handle -> Builder -> IO ()
hPutBuilder h (Builder _ b) = BSB.hPutBuilder h b

hPutBuilderAt :: Handle -> WideInt -> Builder -> IO ()
hPutBuilderAt h pos (Builder _ b) = hSeek h AbsoluteSeek (fromIntegral pos) >> BSB.hPutBuilder h b

writeByte :: Word8 -> Builder
writeByte w = Builder 1 (BSB.word8 w)

writeBytes :: [Word8] -> Builder
writeBytes = mconcat . map writeByte

lazyByteString :: BS.ByteString -> Builder
lazyByteString bs = Builder (fromIntegral $ BS.length bs) (BSB.lazyByteString bs)

-------------------------------------------------------------------------------
-- Encoding and decoding of various values.

encode1ByteLim :: WideInt
encode1ByteLim = 236

encode2BytesN :: WideInt
encode2BytesN = 8

encode2ByteLim :: WideInt
encode2ByteLim = 256*encode2BytesN+encode1ByteLim

encode3ByteLim :: WideInt
encode3ByteLim = 256*encode2BytesN+encode1ByteLim+256*256

encodeWideInt :: WideInt -> Builder
encodeWideInt n
	| n < encode1ByteLim = writeByte $ fromIntegral n
	| n < encode2ByteLim = let n' = n - encode1ByteLim in
		writeBytes [fromIntegral $ encode1ByteLim+shiftR n' 8, fromIntegral n']
	| n < encode3ByteLim = let n' = n - encode2ByteLim in
		writeBytes [fromIntegral $ encode1ByteLim+encode2BytesN, fromIntegral $ shiftR n' 8, fromIntegral n']
	| otherwise = error "encoding!"

decodeWideInt :: BS.ByteString -> (WideInt, BS.ByteString)
decodeWideInt bs = case split of
	Nothing -> (0,bs)	-- safe choice. you can count on it.
	Just (n,bs')
		| n < fromIntegral encode1ByteLim -> (fromIntegral n, bs')
		| n < fromIntegral (encode1ByteLim+encode2BytesN) ->
			decode 1 encode1ByteLim
		| n == fromIntegral (encode1ByteLim+encode2BytesN) ->
			decode 2 encode2ByteLim
		| n < fromIntegral (encode1ByteLim+encode2BytesN+6) -> decode (n-fromIntegral (encode1ByteLim+encode2BytesN)) 0
		| otherwise -> decode ((n-fromIntegral (encode1ByteLim+encode2BytesN+6))*2+8) 0
		where
			decode len add = (v+add, bs'')
				where
					v = BS.foldl' (\n w -> n*256+fromIntegral w) 0 hd
					(hd,bs'') = BS.splitAt (fromIntegral len) bs'
	where
		split = BS.uncons bs

decodeNeedBytes :: BS.ByteString -> Int
decodeNeedBytes bs
	| BS.null bs = 1
	| n < encode1ByteLim = 1
	| n < twoN = 2
	| n < threeN = 3
	| n < eightN = fromIntegral (n-threeN+4)
	| otherwise = fromIntegral ((n-eightN+1)*2+8)
	where
		oneN = encode1ByteLim
		twoN = encode1ByteLim + encode2BytesN
		threeN = encode1ByteLim + encode2BytesN + 1
		eightN = threeN+8-3
		n = fromIntegral $ BS.head bs

decodeNeedBuilderBytes :: Builder -> Int
decodeNeedBuilderBytes bld = decodeNeedBytes $ toLazyByteString bld

encodeString :: String -> Builder
encodeString = mconcat . map (encodeWideInt . fromIntegral . fromEnum)

encodeByteStringWithLength :: Int -> WideInt -> BS.ByteString -> Builder
encodeByteStringWithLength shift add bs = mconcat [encodeWideInt (shiftL (fromIntegral $ BS.length bs) shift + add), lazyByteString bs]

-------------------------------------------------------------------------------
-- Internal API.

changeCounts :: WideInt -> BlockCounts -> BlockCounts -> BlockCounts
changeCounts change addrs blockCounts = Map.mergeWithKey
	(\a count change -> let r = count+change in if r <= 0 then Nothing else Just r)
	id
	(if change > 0 then id else const Map.empty)
	blockCounts (Map.map (*change) addrs)

mkLSM :: LSMInfo -> LSMCmdChan -> Handle -> WideInt -> LSM
mkLSM info chan handle filePages = let lsm = LSM {
		  lsmInfo		= info
		, lsmCmdChan		= chan
		, lsmBlockCounts	= changeCounts 1 (lsmInfoAddrs info) Map.empty
		, lsmFreeBlocks		= error "free blocks!"
		, lsmHandle		= handle
		, lsmPhysPagesStart	= lsmhStateRecPages (lsmHeader lsm) * fromIntegral (length $ lsmiStates $ lsmInfo lsm) + 1
		, lsmBranching		= 1024
		, lsmPagePreread	= 4
		}
	in lsm
	where
		logicalPages = max 0 (filePages - lsmInfoHeaderPages info)

lsmPutBuilderAtPhysPage :: LSM -> WideInt -> WideInt -> Builder -> IO Builder
lsmPutBuilderAtPhysPage lsm physPages pagesCount bb@(Builder bl b) = do
	putStrLn $ "writing "++show (toLazyByteString bb)++" at "++printf "%08x" pos
	hSeek (lsmHandle lsm) AbsoluteSeek $ fromIntegral pos
	BS.hPut (lsmHandle lsm) toWrite
	return rest
	where
		(toWrite, toReturn) = BS.splitAt (fromIntegral len) $ BSB.toLazyByteString b
		rest = lazyByteString toReturn
		pos = lsmPagesBytes lsm physPages
		len = min (fromIntegral bl) $ lsmPagesBytes lsm pagesCount

writeHeader :: LSM -> IO ()
writeHeader lsm = do
	lsmPutBuilderAtPhysPage lsm 0 1 enc
	return ()
	where
		hdr = lsmHeader lsm
		enc = mconcat
			[ encodeWideInt (lsmhVersion hdr)
			, encodeWideInt (fromIntegral $ lsmhPageShift hdr)
			, encodeWideInt (lsmhStatesCount hdr)
			, encodeWideInt (lsmhStateRecSize hdr)
			]

readLogicalPages :: LSM -> WideInt -> WideInt -> IO BS.ByteString
readLogicalPages lsm addr npages = readPhysicalPages lsm (addr + lsmPhysPagesStart lsm) npages

readPhysicalPages :: LSM -> WideInt -> WideInt -> IO BS.ByteString
readPhysicalPages lsm addr npages = do
	hSeek (lsmHandle lsm) AbsoluteSeek $ fromIntegral pos
	BS.hGet (lsmHandle lsm) $ fromIntegral len
	where
		pos = lsmPagesBytes lsm addr
		len = lsmPagesBytes lsm npages

writeState :: Bool -> LSM -> LSMState -> IO ()
writeState flush lsm state = do
	putStrLn $ "Writing state "++show state
	putStrLn $ "         page "++show page
	putStrLn $ "        pages "++show pages
	putStrLn $ "      content "++show (toLazyByteString enc)
	lsmPutBuilderAtPhysPage lsm page pages enc
	when flush $ hFlush $ lsmHandle lsm
	where
		page = lsmStatePage lsm $ lsmsPhysIndex state
		pages = lsmStateRecPages lsm
		levels = lsmsLevels state
		enc = mconcat
			[ encodeWideInt $ lsmsSeqIndex state
			, encList encLevel levels
			]
		encList :: (a -> Builder) -> [a] -> Builder
		encList enc l = mconcat [encodeWideInt (fromIntegral $ length l), mconcat $ map enc l]
		encLevel level = mconcat
			[ encodeWideInt $ lsmlPairsCount level
			, encodeWideInt $ lsmlKeysSize level
			, encodeWideInt $ lsmlDataSize level
			, encList encRun $ lsmlRuns level
			]
		encRun run = encList encBlock $ lsmrBlocks run
		encBlock (LSMBlock a size) = mconcat [encodeWideInt a, encodeWideInt size]

writeStates :: LSM -> IO ()
writeStates lsm = do
	forM_ (lsmiStates $ lsmInfo lsm) $ \s -> writeState False lsm s
	hFlush (lsmHandle lsm)

-- |Internal commands.
data Command =
		CopyLevels	(MVar [LSMLevel])
	|	Close	(MVar ())
	|	Rollback	[LSMLevel]
	|	Commit	Isolation	WideInt	WideInt	WideInt
			(Map.Map BS.ByteString (Maybe BS.ByteString)) [LSMLevel] (MVar ())

internalCopyLevels :: LSMCmdChan -> IO [LSMLevel]
internalCopyLevels chan = do
	result <- newEmptyMVar
	writeChan chan $ CopyLevels result
	takeMVar result

-------------------------------------------------------------------------------
-- Transaction types.

data Isolation = ReadCommitted | SnapshotIsolation
	deriving (Eq, Ord, Show)

newtype LSMTxn = LSMTxn (IORef (Maybe LSMTxnInternal))

data LSMTxnInternal = LSMTxnInternal {
	  lsmtiChan		:: LSMCmdChan
	, lsmtiIsolation	:: Isolation
	, lsmtiAbove		:: Maybe LSMTxn
	, lsmtiMemory		:: Map.Map BS.ByteString (Maybe BS.ByteString)
	, lsmtiMemKeysCount	:: WideInt
	, lsmtiMemKeysSize	:: WideInt
	, lsmtiMemDataSize	:: WideInt
	, lsmtiMemThreshold	:: WideInt
	, lsmtiLevels		:: [LSMLevel]
	}

withTxn :: String -> LSMTxn -> (LSMTxnInternal -> IO (Maybe LSMTxnInternal, a)) -> IO a
withTxn opname (LSMTxn txnRef) act = do
	mbTxni <- readIORef txnRef
	case mbTxni of
		Just txni -> do
			(txn', a) <- act txni
			writeIORef txnRef txn'
			return a
		Nothing -> error $ "closed transaction in "++opname

-- |Reader from disk or memory.
data Iter = 
		IMem	(Map.Map BS.ByteString (Maybe BS.ByteString))
	-- disk data iterator. whether data contains deletes, a buffer to look for data, blocks to read.
	|	IDisk	Bool	Builder [LSMBlock]
	deriving Show

readIterValue :: LSM -> Iter -> IO (Maybe ((BS.ByteString, Maybe BS.ByteString), Iter))
readIterValue lsm (IMem map)
	| Just (kv,map') <- Map.minViewWithKey map = return $ Just (kv, IMem map')
	| otherwise = return Nothing
readIterValue lsm (IDisk hasDels buf blocks)
	| builderNull buf && null blocks = return Nothing
	| canReadLength = do
		error "length read!"
	| otherwise = do
		(buf', blocks') <- rdPart buf blocks
		readIterValue lsm (IDisk hasDels buf' blocks')
	where
		canReadLength = builderLength buf >= decodeNeedBuilderBytes buf
		rdPart buf (LSMBlock a size : blocks) = do
			incr <- readLogicalPages lsm a n
			return (mappend buf (lazyByteString incr), blocks')
			where
				blocks'
					| size' > 0 = LSMBlock (a+n) size' : blocks
					| otherwise = blocks
				size' = size-n
				n = min size 4

-- |Writer to disk or memory.
data Write = Write {
	  wrAllocPagesCount	:: WideInt	-- how many pages allocate to a next block. calculated in advance and does not change.
	, wrAbsPageIndex	:: WideInt	-- index of page as if they all are continuous.
	, wrPageOffset		:: WideInt	-- offset within current page.
	, wrKeysWritten		:: WideInt	-- number of keys written (data may be missing or not data (reference)).
	, wrBlocks		:: [LSMBlock]	-- blocks allocated for a writer. writer fills the last one.
	, wrBuffer		:: Builder	-- buffer of data to write.
	}
	deriving Show

mergeProcess :: LSM -> (WideInt, WideInt, WideInt, Map.Map BS.ByteString (Maybe BS.ByteString)) -> [LSMLevel] -> [LSMLevel] -> IO (LSM, LSMLevel, [LSMLevel])
mergeProcess lsm memPart@(memCnt, memKS, memDS, map) newLevels oldLevels = do
	putStrLn $ "mergeProcess:     mem: "++show memPart
	putStrLn $ "                  new: "++show newLevels
	putStrLn $ "                  old: "++show newLevels
	putStrLn $ "                merge: "++show resultIters
	putStrLn $ "                  rem: "++show rem
	putStrLn $ "     merge no deletes: "++show mergeNoDeletes
	putStrLn $ "              writers: "++show writers
	putStrLn $ "starting priority queue."
	prioQ <- foldM readFirstValue Map.empty resultIters
	(newLevel, lsm) <- merge lsm writers prioQ
	return $ error "aaa!!!"
	where
		merge lsm writers prioQ = case Map.minViewWithKey prioQ of
			Just ((key,(n, mbVal, iter)), prioQ') -> do
				(writers', lsm') <- if (not mergeNoDeletes || Maybe.isJust mbVal)
					then writeWriters lsm key mbVal writers
					else return (writers, lsm)
				prioQ' <- putIter n iter prioQ
				merge lsm' writers' prioQ'
			Nothing -> finalizeMerge lsm writers
		finalizeMerge lsm writers = error "finalize merge!"
		writeWriters :: LSM -> BS.ByteString -> Maybe BS.ByteString -> [Write] -> IO ([Write], LSM)
		writeWriters lsm key mbVal (wr:wrs) = do
			writers' <- go False mbVal wr wrs
			return (writers', error "writers lsm!")
			where
				keyEnc = encodeByteStringWithLength 0 0 key
				writeIndexSeq mbValue wr = do
					let	value = Maybe.fromMaybe (error "nothing for index seq?") mbValue
						add = mconcat [keyEnc, encodeByteStringWithLength 0 0 value]
					return $ wr {
						  wrBuffer = mappend (wrBuffer wr) add
						}
				writeKeyDataSeq mbValue wr = do
					let	specialFlag = Maybe.fromMaybe True $ fmap BS.null mbValue
						deleteFlag = not mergeNoDeletes && Maybe.isNothing mbValue
						addToLen = (if specialFlag then 1 else 0) + (if deleteFlag then 2 else 0)
						shift = if mergeNoDeletes then 1 else 2
						keyEnc = encodeByteStringWithLength shift addToLen key
						dataEnc = Maybe.fromMaybe mempty $ fmap (\v -> if BS.null v then mempty else encodeByteStringWithLength 0 0 v) mbVal
					return $ wr {
						  wrBuffer = mconcat [wrBuffer wr, keyEnc, dataEnc]
						, wrKeysWritten = let kw' = wrKeysWritten wr + 1 in
							if kw' >= branching then kw' - branching else kw'
						}
				go indexWr mbValue wr [] = do
					wr' <- (if indexWr then writeIndexSeq else writeKeyDataSeq)
						mbValue wr
					return [wr']
				go indexWr mbValue wr (iwr:iwrs) = do
					let	nextData = Just $ encodePos wr
					wrs <- go True nextData iwr iwrs
					wr <- if indexWr
						then writeIndexSeq mbValue wr
						else writeKeyDataSeq mbValue wr
					return $ wr : wrs
		encodePos wr = error "encode pos!"
		putIter n iter prioQ = do
			readResult <- readIterValue lsm iter
			case readResult of
				Just (key, mbVal) -> error "some read!"
					where
						
				Nothing -> return prioQ
		mergeNoDeletes = null rem
		readFirstValue map (n,iter) = do
			val <- readIterValue lsm iter
			putStrLn $ "first value. iter: "++show iter
			putStrLn $ "                n: "++show n
			putStrLn $ "              val: "++show val
			case val of
				Nothing -> return map
				Just ((key, mbVal), iter')
					-- the value for that key was added before by more recent reader.
					| Map.member key map -> readFirstValue map (n,iter')
					-- we have a spot to fill!
					| otherwise -> return $ Map.insert key (n,mbVal, iter') map
		branching = lsmBranching lsm
		up x = div (x+branching-1) branching
		inPages x = lsmBytesPages lsm x
		writers = makeWriters False resultCount resultSize
		makeWriters indexWriter cnt allDataSize
			| indexWriter && cnt < lsmPagePreread lsm = []
			| otherwise = writer : makeWriters True cntUp allDataSizeUp
			where
				allocPagesCount = div (inPages allDataSize + 7) 8
				writer = Write {
					  wrAllocPagesCount	= allocPagesCount
					, wrAbsPageIndex	= 0
					, wrPageOffset		= 0
					, wrKeysWritten		= 0
					, wrBlocks		= []
					, wrBuffer		= mempty
					}
				cntUp = up cnt
				allDataSizeUp = up allDataSize
		forceMergeAllNew = not (null newLevels) && not (null oldLevels)
			&& levelAllDataSize (last newLevels) > levelAllDataSize (head oldLevels)
		resultIters = zip [1..] $ reverse resultItersRev
		((resultCount, resultSize, resultItersRev), rem)
			| null afterNewRest = computeMerge True False afterNewIters oldLevels
			| otherwise = (afterNewIters, afterNewRest++oldLevels)
			where
				(afterNewIters, afterNewRest) =
					computeMerge (null oldLevels) forceMergeAllNew (memCnt, (memKS+memDS), [IMem map]) newLevels
		computeMerge lastLevelLast forceAdd iters [] = (iters, [])
		computeMerge lastLevelLast forceAdd iters@(cnt, size, itersRev) rest@(level:levels)
			| add = computeMerge lastLevelLast forceAdd (cnt+lcnt, size+lsize, IDisk hasDeletes mempty lblocks : itersRev) levels
			| otherwise = (iters, rest)
			where
				add = forceAdd || lsize <= 2 * size
				lsize = levelAllDataSize level
				lcnt = lsmlPairsCount level
				hasDeletes = not lastLevelLast || not (null levels)
				lblocks = lsmrBlocks $ last $ lsmlRuns level
		levelAllDataSize l = lsmlKeysSize l + lsmlDataSize l

acquireReleaseBlocks :: LSM -> [LSMBlock] -> [LSMBlock] -> LSM
acquireReleaseBlocks lsm acquire release = rlsm
	where
		alsm = List.foldl' acq lsm acquire
		rlsm = List.foldl' rel alsm release
		acq lsm (LSMBlock a s) = lsm'
			where
				lsm' = lsm {
					  lsmBlockCounts = Map.insertWith (+) a 1 $ lsmBlockCounts lsm
					}
		rel lsm b@(LSMBlock a s)
			| Just n <- old, n > 1 = lsm { lsmBlockCounts = counts }
			| Just n <- old, n <= 1 = lsm { lsmBlockCounts = Map.delete a counts, lsmFreeBlocks = mergeFree (lsmFreeBlocks lsm) [b]}
			where
				(old, counts) = Map.insertLookupWithKey (\_ o c -> o+c) a (-1) $ lsmBlockCounts lsm

acquireReleaseLevels :: LSM -> [LSMLevel] -> [LSMLevel] -> LSM
acquireReleaseLevels lsm acquire release = acquireReleaseBlocks lsm (lsmLevelsBlocks acquire) (lsmLevelsBlocks release)

mergeFree [] released = released
mergeFree free [] = free
mergeFree a@(lb1:lb1s) b@(lb2:lb2s)
	| t1 < a2 = lb1 : mergeFree lb1s b
	| t2 < a1 = lb2 : mergeFree a lb2s
	| t1 == a2 = mergeFree (LSMBlock a1 (lsmbSize lb1 + lsmbSize lb2) : lb1s) lb2s
	| t2 == a1 = mergeFree (LSMBlock a2 (lsmbSize lb1 + lsmbSize lb2) : lb1s) lb2s
	| otherwise = error $ "blocks are not disjoint or adjacent.\na: "++show a++"\nb: "++show b
	where
		a1 = lsmbAddr lb1
		a2 = lsmbAddr lb2
		t b = lsmbAddr b+lsmbSize b
		t1 = t lb1
		t2 = t lb2

replaceOldestState :: LSM -> [LSMLevel] -> IO LSM
replaceOldestState lsm levels = do
	writeState True lsm new
	return $ lsm {
		  lsmInfo = let info = lsmInfo lsm in info {
			  lsmiStates = map toNew $ lsmiStates info
			}
		}
	where
		toNew s
			| lsmsPhysIndex s == lsmsPhysIndex new = new
			| otherwise = s
		statesSeqI = map (\s -> (lsmsSeqIndex s, s)) $ lsmiStates $ lsmInfo lsm
		oldest = snd $ minimum statesSeqI
		new = oldest {
			  lsmsSeqIndex = lsmsSeqIndex oldest + 1
			, lsmsLevels = levels
			}

-------------------------------------------------------------------------------
-- Worker loop.

lsmWorker :: LSM -> IO ()
lsmWorker lsm = do
	cmd <- readChan (lsmCmdChan lsm)
	case cmd of
		CopyLevels result -> do
			error "copy levels!"
			let	lsm' = error "lsm' in copy levels!"
			lsmWorker lsm'
		Rollback levels -> do
			let	blocksToRelease = Map.unions $ map (\b -> Map.singleton (lsmbAddr b) (1, lsmbSize b)) $
					concatMap lsmrBlocks $ concatMap lsmlRuns levels
				blocksRemain = Map.mergeWithKey
					(\_a cnt (dec, size) -> let r = cnt - dec in if r < 1 then Nothing else Just r)
					id
					(error . ("blocks to free that are not in alloced blocks: " ++) . show)
					(lsmBlockCounts lsm) blocksToRelease
				releasedBlocks = Map.toList $ Map.difference blocksToRelease blocksRemain
				freeBlocks = mergeFree (lsmFreeBlocks lsm) $ map (\(a,(_,size)) -> LSMBlock a size) releasedBlocks
				lsm' = lsm {
					  lsmBlockCounts	= blocksRemain
					, lsmFreeBlocks		= freeBlocks
					}
			lsmWorker lsm'
		Close lock -> do
			hClose $ lsmHandle lsm
			putMVar lock ()
			return ()
		Commit isolation memCounts memKeysSize memDataSize memoryPart levels lock -> do
			let	recentLevels = findRecentLevels lsm
			lsm' <- case isolation of
				ReadCommitted
					-- no change.
					| memCounts == 0 && null levels -> return lsm
					-- merge levels and/or mem.
					| otherwise -> do
						(lsm', mergedLevel, remainingLevels) <- mergeProcess lsm (memCounts, memKeysSize, memDataSize, memoryPart) levels recentLevels
						let	lsm'' = acquireReleaseLevels lsm' (mergedLevel:remainingLevels) (levels ++ recentLevels)
						replaceOldestState lsm' (mergedLevel : remainingLevels)
				SnapshotIsolation
					-- no change.
					| memCounts == 0 && levels == recentLevels -> return lsm
					-- everything is on disk (can happen).
					| memCounts == 0 -> error "SI: need to change levels!"
					-- performing merge.
					| otherwise -> error "SI: need to merge with mem!"
			putMVar lock ()
			lsmWorker lsm'

-------------------------------------------------------------------------------
-- The external API.


-- |Create/open existing LSM file with specified parameters.
-- You probably should leave pageBits at 13 (8K bytes), it is good enough and
-- won't affect performance much.
-- The more states you keep in file (determined by statesCount), the more durable DB is.
--
newLSMWithConfig :: Int -> Int -> Bool -> FilePath -> IO LSMCmdChan
newLSMWithConfig pageBits statesCount forceCreate path
	| finiteBitSize (1::Int) < finiteBitSize (1::WideInt) = error "need 64+-bit platform."
	| otherwise = do
	ch <- newChan
	lsm <- do
		h <- openBinaryFile path ReadWriteMode
		size <- hFileSize h
		if forceCreate
			then do
				let	info = emptyLSMInfo pageBits (fromIntegral statesCount)
					pages = fromIntegral $ div size (fromIntegral $ lsmInfoPageSize info)
					lsm = mkLSM info ch h pages
				writeHeader lsm
				writeStates lsm	-- this syncs. ;)
				return lsm
			else error "reading LSM info!"
	forkIO $ lsmWorker lsm
	return $ lsmCmdChan lsm

-- |Create/openexisting LSM file with default parameters.
newLSM :: Bool -> FilePath -> IO LSMCmdChan
newLSM forceCreate fn = newLSMWithConfig defaultPageBits 3 forceCreate fn

-- |Close the LSM.
lsmClose :: LSMCmdChan -> IO ()
lsmClose chan = do
	lock <- newEmptyMVar
	writeChan chan $ Close lock
	readMVar lock

-- |Begin a top-level transaction.
lsmBegin :: Isolation -> LSMCmdChan -> IO LSMTxn
lsmBegin isolLevel chan = do
	levels <- case isolLevel of
		SnapshotIsolation -> internalCopyLevels chan
		ReadCommitted -> return []
	liftM LSMTxn $ newIORef $ Just $ txni {
		  lsmtiLevels		= levels
		}
	where
		txni = LSMTxnInternal {
			  lsmtiChan		= chan
			, lsmtiIsolation	= isolLevel
			, lsmtiAbove		= Nothing
			, lsmtiMemory		= Map.empty
			, lsmtiMemKeysCount	= 0
			, lsmtiMemKeysSize	= 0
			, lsmtiMemDataSize	= 0
			, lsmtiLevels		= error "txni levels unset"
			, lsmtiMemThreshold	= defaultMemoryPartSize
			}


-- |Begin a nested transaction.
lsmNest :: LSMTxn -> IO LSMTxn
lsmNest txn = do
	error "beginnest!"

-- |Commit a transaction. Any attempt to do anything past commit/rollback will result in error.
-- Please do not leave dangling transactions (not committed/rolled back). They would create memory/other info leak.
lsmCommit :: LSMTxn -> IO ()
lsmCommit txn = withTxn "lsmCommit" txn $ \txni -> do
	lock <- newEmptyMVar
	writeChan (lsmtiChan          txni) $
		Commit	(lsmtiIsolation txni)
			(lsmtiMemKeysCount txni) (lsmtiMemKeysSize txni) (lsmtiMemDataSize txni)
			(lsmtiMemory txni) (lsmtiLevels txni) lock
	readMVar lock
	return (Nothing, ())

-- |Rollback (abort) a transaction.
lsmRollback :: LSMTxn -> IO ()
lsmRollback txn = withTxn "lsmRollback" txn $ \txni -> do
	writeChan (lsmtiChan txni) $ Rollback $ lsmtiLevels txni
	return (Nothing, ())

lsmWriteInternal :: LSMTxn -> BS.ByteString -> Maybe BS.ByteString -> IO ()
lsmWriteInternal txn key value = withTxn "lsmWrite" txn $ \txni -> do
	let	(old, newMem) = Map.insertLookupWithKey (\_ a _ -> a) key value $ lsmtiMemory txni
		txni' = case old of
			Just oldv -> txni {
				  lsmtiMemDataSize = lsmtiMemDataSize txni + vlen value - vlen oldv
				, lsmtiMemory = newMem
				}
			Nothing -> txni {
				  lsmtiMemDataSize = lsmtiMemDataSize txni + vlen value
				, lsmtiMemKeysSize = lsmtiMemKeysSize txni + BS.length key
				, lsmtiMemKeysCount = lsmtiMemKeysCount txni+1
				, lsmtiMemory = newMem
				}
	txni'' <- if lsmtiMemDataSize txni' + lsmtiMemKeysSize txni' >= lsmtiMemThreshold txni'
		then error "flush!"
		else return txni'
	return (Just txni'', ())
	where
		vlen = maybe 0 BS.length

lsmWrite :: LSMTxn -> BS.ByteString -> BS.ByteString -> IO ()
lsmWrite txn key value = lsmWriteInternal txn key (Just value)

lsmDelete :: LSMTxn -> BS.ByteString -> IO ()
lsmDelete txn key = lsmWriteInternal txn key Nothing

lsmRead :: LSMTxn -> BS.ByteString -> IO (Maybe BS.ByteString)
lsmRead txn key = withTxn "lsmRead" txn $ \txni -> do
	val <- case Map.lookup key $ lsmtiMemory txni of
		Just v -> return v
		Nothing -> error "read on disk!"
	return (Just txni, val)
