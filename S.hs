{-# LANGUAGE ForeignFunctionInterface, PatternGuards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
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

import Data.Either (isRight)

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
-- Obligatory internal error, unsupported feature and not-yet-done errors.

internal, unsupported, ned :: String -> a
unsupported msg = error $ "unsupported: "++msg
internal msg = error $ "internal error: "++msg
ned msg = error $ "not yet done: "++msg

debugMode :: Bool
debugMode = False

debugPutStrLn :: String -> IO ()
debugPutStrLn
	| debugMode = putStrLn
	| otherwise = const $ return ()

-------------------------------------------------------------------------------
-- Constants.

defaultPageBits :: Int
defaultPageBits = 13

defaultMemoryPartSize :: WideInt
defaultMemoryPartSize = 256*1024

defaultFlushSize :: WideInt
defaultFlushSize = 8*1024*1024

minSizeIncrement :: WideInt
minSizeIncrement = defaultMemoryPartSize*2

defaultBranchingFactor :: WideInt
defaultBranchingFactor = 1024

defaultCommitRecordSize :: WideInt
defaultCommitRecordSize = 32*1024

-------------------------------------------------------------------------------
-- Data types.

newtype WideInt = WideInt Int64
	deriving (Eq, Ord, Num, Integral, Enum, Real, Show, Bits, PrintfArg, FiniteBits)

bystrLen :: BS.ByteString -> WideInt
bystrLen = fromIntegral . BS.length

type BlockCounts = Map.Map WideInt (WideInt, LSMBlock)

data LSMInfo = LSMInfo {
	  lsmiHeader		:: LSMHeader
	, lsmiStates		:: [LSMState]
	}
	deriving Show

emptyLSMInfo :: Int -> WideInt -> LSMInfo
emptyLSMInfo pageBits statesCount = LSMInfo h (map emptyLSMState [0 .. lsmhStatesCount h-1])
	where
		h = defaultLSMHeader pageBits statesCount

_checkLength :: (WideInt, LSMBlock) -> (WideInt, LSMBlock) -> (WideInt, LSMBlock)
_checkLength (a,ablk) (b, bblk)
	| lsmbSize ablk == lsmbSize bblk = (a, ablk)
	| otherwise = internal $ "different lengths for same address: a "++show a++" and b "++show b

_allAddrs :: (b -> BlockCounts) -> (a -> [b]) -> a -> BlockCounts
_allAddrs g f = Map.unionsWith _checkLength . map g . f

lsmInfoCounts :: LSMInfo -> BlockCounts
lsmInfoCounts = _allAddrs lsmStateCounts lsmiStates

lsmStateCounts :: LSMState -> BlockCounts
lsmStateCounts = _allAddrs lsmLevelCounts lsmsLevels

lsmLevelCounts :: LSMLevel -> BlockCounts
lsmLevelCounts = _allAddrs lsmRunCounts lsmlRuns

lsmRunCounts = _allAddrs (\b@(LSMBlock a l) -> Map.singleton a (1,b)) lsmrBlocks

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
	| pageBits < 6 = unsupported "page size less than 64 is not supported."
	| otherwise = LSMHeader 0 pageBits (shiftL 1 pageBits) statesCount defaultCommitRecordSize (shiftR defaultCommitRecordSize pageBits)

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
	, lsmlHasDeletes	:: Bool
	, lsmlRuns		:: [LSMRun]
	}
	deriving (Eq, Ord, Show)

data LSMRunType = Sequential
	deriving (Eq, Ord, Show, Enum)

encodeLSMRunType :: LSMRunType -> Builder
encodeLSMRunType Sequential = encodeWideInt 0

decodeLSMRunType :: BS.ByteString -> (LSMRunType, BS.ByteString)
decodeLSMRunType bs = (ty, bs')
	where
		ty = case n of
			0 -> Sequential
			_ -> unsupported $ "unknown run type code "++show n
		(n,bs') = decodeWideInt bs

data LSMRun = LSMRun {
	  lsmrType	:: LSMRunType
	, lsmrBlocks	:: [LSMBlock]
	}
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
	, lsmFlushThreshold	:: WideInt
	}

findRecentLevels :: LSM -> [LSMLevel]
findRecentLevels lsm = lsmsLevels $ findMostRecent $ lsmiStates $ lsmInfo lsm
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
	| n < shiftL 1 24 = bytesAndCode 1 16
	| n < shiftL 1 32 = bytesAndCode 2 24
	| n < shiftL 1 40 = bytesAndCode 3 32
	| n < shiftL 1 48 = bytesAndCode 4 40
	| n < shiftL 1 56 = bytesAndCode 5 48
	| otherwise = bytesAndCode 6 56
	where
		bytesAndCode l shift = writeBytes $ (fromIntegral $ l+encode1ByteLim+encode2BytesN) : bytes shift
		bytes shift
			| shift == 0 = [fromIntegral n]
			| otherwise = fromIntegral (shiftR n shift) : bytes (shift-8)

decodeWideInt :: BS.ByteString -> (WideInt, BS.ByteString)
decodeWideInt bs = case split of
	Nothing -> (0,bs)	-- safe choice. you can count on it.
	Just (n,bs')
		| n < fromIntegral encode1ByteLim -> (fromIntegral n, bs')
		| n < fromIntegral (encode1ByteLim+encode2BytesN) ->
			let m = fromIntegral n - encode1ByteLim in decode 1 (encode1ByteLim + m * 256)
		| n == fromIntegral (encode1ByteLim+encode2BytesN) ->
			decode 2 encode2ByteLim
		| n < fromIntegral (encode1ByteLim+encode2BytesN+6) -> decode (n-fromIntegral (encode1ByteLim+encode2BytesN)+3) 0
		| otherwise -> decode ((n-fromIntegral (encode1ByteLim+encode2BytesN+6))*2+8) 0
		where
			decode len add = (v+add, bs'')
				where
					v = BS.foldl' (\n w -> n*256+fromIntegral w) 0 hd
					(hd,bs'') = BS.splitAt (fromIntegral len) bs'
	where
		split = BS.uncons bs

decodeNeedBytes :: BS.ByteString -> WideInt
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

type DecodeM a = State BS.ByteString a

decodeWideIntM :: DecodeM WideInt
decodeWideIntM = do
	s <- get
	let	(r,s') = decodeWideInt s
	put s'
	return r

decodeListM :: DecodeM a -> DecodeM [a]
decodeListM decoder = do
	n <- decodeWideIntM
	liftM reverse $ foldM (\list _ -> decoder >>= \x -> return (x:list)) [] [1..n]

decodeListMapM :: (a -> DecodeM b) -> [a] -> DecodeM [b]
decodeListMapM decoder list = do
	n <- decodeWideIntM
	liftM reverse $ foldM (\list a -> decoder a >>= \x -> return (x:list)) [] $ zipWith const list [1..n]

decodeLSMRunTypeM :: DecodeM LSMRunType
decodeLSMRunTypeM = do
	s <- get
	let	(ty,s') = decodeLSMRunType s
	put s'
	return ty

decodeNeedBuilderBytes :: Builder -> WideInt
decodeNeedBuilderBytes bld = decodeNeedBytes $ toLazyByteString bld

encodeString :: String -> Builder
encodeString = mconcat . map (encodeWideInt . fromIntegral . fromEnum)

encodeLength :: Int ->WideInt -> WideInt -> Builder
encodeLength shift add len = encodeWideInt (shiftL len shift + add)

-------------------------------------------------------------------------------
-- Internal API.

changeCounts :: WideInt -> BlockCounts -> BlockCounts -> BlockCounts
changeCounts change addrs blockCounts = Map.mergeWithKey
	(\a (count, b) (change,b') -> let r = count+change in if r <= 0 then Nothing else Just (r,b))
	id
	(if change > 0 then id else const Map.empty)
	blockCounts (Map.map (\(c,b) -> (c*change, b)) addrs)

mkLSM :: LSMInfo -> LSMCmdChan -> Handle -> WideInt -> LSM
mkLSM info chan handle filePages = let lsm = LSM {
		  lsmInfo		= info
		, lsmCmdChan		= chan
		, lsmBlockCounts	= counts
		, lsmFreeBlocks		= freeBlocks 0 $ map snd $ Map.elems counts
		, lsmHandle		= handle
		, lsmPhysPagesStart	= lsmhStateRecPages (lsmHeader lsm) * fromIntegral (length $ lsmiStates $ lsmInfo lsm) + 1
		, lsmBranching		= defaultBranchingFactor
		, lsmPagePreread	= 4
		, lsmFlushThreshold	= defaultFlushSize
		}
	in lsm
	where
		counts = lsmInfoCounts info
		freeBlocks lastFree [] = []
		freeBlocks lastFree (b:bs)
			| lastFree < lsmbAddr b = LSMBlock lastFree size
				: rest
			| otherwise = rest
			where
				rest = freeBlocks (lsmbAddr b + lsmbSize b) bs
				size = lsmbAddr b - lastFree
		logicalPages = max 0 (filePages - lsmInfoHeaderPages info)

lsmPutBuilderAtPhysPage :: LSM -> WideInt -> WideInt -> Builder -> IO Builder
lsmPutBuilderAtPhysPage lsm physPages pagesCount bb@(Builder bl b) = do
	debugPutStrLn $ "writing "++show (toLazyByteString bb)++" at "++printf "%08x" pos
	hSeek (lsmHandle lsm) AbsoluteSeek $ fromIntegral pos
	BS.hPut (lsmHandle lsm) toWrite
	return rest
	where
		(toWrite, toReturn) = BS.splitAt (fromIntegral len) $ BSB.toLazyByteString b
		rest = lazyByteString toReturn
		pos = lsmPagesBytes lsm physPages
		len = min (fromIntegral bl) $ lsmPagesBytes lsm pagesCount

lsmPutBuilderAtLogicalPage :: LSM -> WideInt -> WideInt -> Builder -> IO Builder
lsmPutBuilderAtLogicalPage lsm logicalStart pagesCount builder =
	lsmPutBuilderAtPhysPage lsm (logicalStart + lsmPhysPagesStart lsm) pagesCount builder


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

readHeaderInfo :: LSM -> IO LSM
readHeaderInfo lsm = do
	lsm' <- readHeader lsm
	let	hdr = lsmiHeader $ lsmInfo lsm'
	when (lsmhVersion hdr /= 0) $ internal "invalid header version!"
	when (lsmhStatesCount hdr > 9 || lsmhStatesCount hdr < 1) $ internal "invalid states count!"
	when (lsmhStateRecSize hdr > 128*1024) $ internal $ "state record size "++show (lsmhStateRecSize hdr)++" is too big."
	when (lsmhStateRecSize hdr < 1024) $ internal $ "state record size "++show (lsmhStateRecSize hdr)++" is too small."
	states <- foldM (\states i -> readState lsm' i >>= \s -> return (states ++[s])) [] [0..lsmhStatesCount hdr - 1]
	debugPutStrLn $ "LSM states read: "++show states
	return $ lsm' { lsmInfo = (lsmInfo lsm') { lsmiStates = states } }
	where
		readState lsm i = do
			statePages <- readPhysicalPages lsm (lsmStatePage lsm i) (lsmStateRecPages lsm)
			let	run = do
					runTy <- decodeLSMRunTypeM
					blocks <- decodeListM $ liftM2 LSMBlock decodeWideIntM decodeWideIntM
					return $ LSMRun runTy blocks
				level = do
					pairsCount <- decodeWideIntM
					keysSize <- decodeWideIntM
					dataSize <- decodeWideIntM
					runs <- decodeListM run
					return $ LSMLevel {
						  lsmlPairsCount	= pairsCount
						, lsmlKeysSize		= keysSize
						, lsmlDataSize		= dataSize
						, lsmlHasDeletes	= False
						, lsmlRuns		= runs
						}
				state = flip evalState statePages $ do
					seqIndex <- decodeWideIntM
					let	setDelete delFlag level = level { lsmlHasDeletes = delFlag }
						addDeletes list = zipWith setDelete
							(tail $ map (const True) list ++ repeat False)
							list
					levels <- decodeListM level
					return $ LSMState {
						  lsmsPhysIndex		= i
						, lsmsSeqIndex		= seqIndex
						, lsmsLevels		= levels
						}
			return state
		readHeader lsm = do
			hdrBytes <- readPhysicalPages lsm 0 1
			let	lsm' = flip evalState hdrBytes $ do
					version <- decodeWideIntM
					pageShift <- liftM fromIntegral decodeWideIntM
					statesCount <- decodeWideIntM
					stateRecSize <- decodeWideIntM
					let	hdr = LSMHeader {
							  lsmhVersion		= version
							, lsmhPageShift		= pageShift
							, lsmhPageSize		= shiftL 1 pageShift
							, lsmhStatesCount	= statesCount
							, lsmhStateRecSize	= stateRecSize
							, lsmhStateRecPages	= lsmBytesPages lsm' stateRecSize
							}
						lsm' = lsm { lsmInfo = (lsmInfo lsm) { lsmiHeader = hdr } }
					return lsm'
			return lsm'

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
	debugPutStrLn $ "Writing state "++show state
	debugPutStrLn $ "         page "++show page
	debugPutStrLn $ "        pages "++show pages
	debugPutStrLn $ "      content "++show (toLazyByteString enc)
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
		encRun (LSMRun ty blocks) = mconcat [encodeLSMRunType ty, encList encBlock blocks]
		encBlock (LSMBlock a size) = mconcat [encodeWideInt a, encodeWideInt size]

writeStates :: LSM -> IO ()
writeStates lsm = do
	forM_ (lsmiStates $ lsmInfo lsm) $ \s -> writeState False lsm s
	hFlush (lsmHandle lsm)

-- |Internal commands.
data Command =
		CopyLevels	(MVar [LSMLevel])
	|	Close	(MVar ())
	|	Release	[LSMBlock]
	|	Commit	Isolation	WideInt	WideInt	WideInt
			(Map.Map BS.ByteString (Maybe BS.ByteString)) [LSMLevel] (MVar ())
	|	ReadIter	DiskIter	(MVar DiskIter)

internalCopyLevels :: LSMCmdChan -> IO [LSMLevel]
internalCopyLevels chan = do
	result <- newEmptyMVar
	writeChan chan $ CopyLevels result
	takeMVar result

internalReleaseLevels :: LSMCmdChan -> [LSMLevel] -> IO ()
internalReleaseLevels chan levels = internalReleaseBlocks chan $ lsmLevelsBlocks levels

internalReleaseBlocks :: LSMCmdChan -> [LSMBlock] -> IO ()
internalReleaseBlocks chan blocks = writeChan chan $ Release blocks

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
		Nothing -> internal $ "closed transaction in "++opname

data DiskIter = DiskIter {
	  diDone		:: Bool
	, diHasDeletes
	, diHasSpecial		:: Bool
	, diPageOffset		:: WideInt
	, diPageIndex		:: WideInt
	, diPagesPreread	:: WideInt
	, diBuffer		:: BS.ByteString
	, diBlocks		:: [LSMBlock]
	, diKVs			:: [(BS.ByteString, Maybe BS.ByteString)]
	}
	deriving Show

-- |Reader from disk or memory.
data Iter = 
		IMem	(Map.Map BS.ByteString (Maybe BS.ByteString))
	-- disk data iterator. whether data contains deletes, a number of pages to preread, a buffer to look for data, blocks to read.
	|	IDisk	DiskIter
	deriving Show

findPageBlockAbs :: WideInt -> [LSMBlock] -> LSMBlock
findPageBlockAbs pageIndex blocks = find pageIndex blocks
	where
		find _ [] = internal $ "pageIndex out of blocks range"++"\npage index: "++show pageIndex ++"\nblocks: "++show blocks
		find i (b:bs)
			| i < lsmbSize b = LSMBlock (lsmbAddr b + i) (lsmbSize b - i)
			| otherwise = find (i-lsmbSize b) bs

readDiskIterValue :: LSM -> DiskIter -> IO (Maybe ((BS.ByteString, Maybe BS.ByteString), DiskIter))
readDiskIterValue lsm (DiskIter done hasDels hasSpecl ofs index pagesPre buffer blocks (kv:kvs)) =
	return $ Just (kv,DiskIter done hasDels hasSpecl ofs index pagesPre buffer blocks kvs)
readDiskIterValue lsm (DiskIter done hasDels hasSpecl ofs index pagesPre buffer blocks [])
	| done = return Nothing
	| canReadLength = do
		ned "length read!"
	| otherwise = do
		(buf', index') <- rdPart buffer
		readDiskIterValue lsm (DiskIter done hasDels hasSpecl 0 index pagesPre buf' blocks [])
	where
		canReadLength = fromIntegral (BS.length buffer) >= decodeNeedBytes buffer
		LSMBlock a size = findPageBlockAbs index blocks
		rdPart buf = do
			incr <- readLogicalPages lsm a n
			return (BS.append buf incr, index+n)
			where
				size' = size-n
				n = min size pagesPre

readIterValue :: LSM -> Iter -> IO (Maybe ((BS.ByteString, Maybe BS.ByteString), Iter))
readIterValue lsm (IMem map) = return $ (fmap $ \(kv,map) -> (kv,IMem map)) $ Map.minViewWithKey map
readIterValue lsm (IDisk diskIter) = do
	liftM (fmap (\(kv,di') -> (kv, IDisk di'))) $ readDiskIterValue lsm diskIter

-- |Writer to disk or memory.
data Writer = Writer {
	  wrAllocPagesCount		:: WideInt	-- how many pages allocate to a next block. calculated in advance and does not change.
	, wrAbsPageIndex		:: WideInt	-- index of page as if they all are continuous.
	, wrKeysWritten			:: WideInt	-- number of keys written (data may be missing or not data (reference)).
	, wrKeysWrittenModBranch	:: WideInt
	, wrKeysSize			:: WideInt	-- size of keys written.
	, wrDataSize			:: WideInt	-- size of data written
	, wrBlocks			:: [LSMBlock]	-- blocks allocated for a writer. writer fills the last one.
	, wrBuffer			:: Builder	-- buffer of data to write.
	}
	deriving Show

writerBytesInBuffer :: Writer -> WideInt
writerBytesInBuffer wr = fromIntegral $ builderLength $ wrBuffer wr

writerPagesInBuffer :: LSM -> Writer -> WideInt
writerPagesInBuffer lsm wr = lsmPagesBytes lsm $ writerBytesInBuffer wr

writerPagesRemains :: Writer -> WideInt
writerPagesRemains wr = sum (map lsmbSize $ wrBlocks wr) - wrAbsPageIndex wr

writerToRun :: Writer -> LSMRun
writerToRun wr = LSMRun {
	  lsmrType	= Sequential
	, lsmrBlocks	= wrBlocks wr
	}

makeLevel :: [LSMRun] -> Bool -> Writer -> LSMLevel
makeLevel runs deletes kdWriter = LSMLevel {
	  lsmlRuns		= runs
	, lsmlPairsCount	= wrKeysWritten kdWriter
	, lsmlKeysSize		= wrKeysSize kdWriter
	, lsmlDataSize		= wrDataSize kdWriter
	, lsmlHasDeletes	= deletes
	}

allocateBlock :: WideInt -> LSM -> (LSM, LSMBlock)
allocateBlock size lsm
	| null goodBlocks = internal "null goodBlocks!"
	| otherwise = (withBestBlock, allocated)
	where
		freeBlocks = lsmFreeBlocks lsm
		goodBlocks = filter ((>= size) . lsmbSize) freeBlocks
		bestBlock = snd $ minimum $ map (\b -> (lsmbSize b - size, b)) goodBlocks
		allocated = bestBlock { lsmbSize = size }
		toChange = bestBlock { lsmbAddr = lsmbAddr bestBlock + size, lsmbSize = lsmbSize bestBlock - size }
		withBestBlock
			| lsmbSize bestBlock > size = lsm {
				  lsmFreeBlocks = map (\b -> if lsmbAddr b == lsmbAddr allocated then toChange else b) $ lsmFreeBlocks lsm
				}
			| otherwise = lsm {
				  lsmFreeBlocks = filter (\b -> lsmbAddr b /= lsmbAddr allocated) $ lsmFreeBlocks lsm
				}

writerBufPages :: Writer -> BS.ByteString
writerBufPages wr = toLazyByteString (wrBuffer wr)

writerPageStart :: Writer -> WideInt
writerPageStart wr = find absPage (wrBlocks wr)
	where
		absPage = wrAbsPageIndex wr
		find index [] = internal $ "writerPageStart: index not found: "++show wr
		find index (b:bs)
			| index < lsmbSize b = lsmbAddr b + index
			| otherwise = find (index - lsmbSize b) bs

mergeProcess :: LSM -> Bool -> (WideInt, WideInt, WideInt, Map.Map BS.ByteString (Maybe BS.ByteString)) -> [LSMLevel] -> [LSMLevel] -> IO (LSM, LSMLevel, [LSMLevel])
mergeProcess lsm leaveDeletes memPart@(memCnt, memKS, memDS, memMap) newLevels oldLevels = do
	debugPutStrLn $ "mergeProcess:     mem: "++show memPart
	debugPutStrLn $ "                  new: "++show newLevels
	debugPutStrLn $ "                  old: "++show newLevels
	debugPutStrLn $ "                merge: "++show resultIters
	debugPutStrLn $ "                  rem: "++show rem
	debugPutStrLn $ "     merge no deletes: "++show mergeNoDeletes
	debugPutStrLn $ "              writers: "++show writers
	debugPutStrLn $ "starting priority queue."
	prioQ <- foldM readFirstValue Map.empty resultIters
	(newLevel, lsm) <- merge lsm writers prioQ
	debugPutStrLn $ "newLevel: "++show newLevel
	debugPutStrLn $ "resulting free blocks: "++show (lsmFreeBlocks lsm)
	debugPutStrLn $ "resulting block counts: "++show (lsmBlockCounts lsm)
	return (lsm, newLevel, rem)
	where
		merge lsm writers prioQ = case Map.minViewWithKey prioQ of
			Just ((key,(n, mbVal, iter)), prioQ') -> do
				(writers', lsm') <- if (not mergeNoDeletes || Maybe.isJust mbVal)
					then writeWriters lsm key mbVal writers
					else return (writers, lsm)
				prioQ' <- putIter n iter prioQ'
				merge lsm' writers' prioQ'
			Nothing -> finalizeMerge lsm writers
		finalizeMerge lsm writers = do
			debugPutStrLn "Finalizing merge."
			(finalWriters, lsm) <- foldM
				(\(ws, lsm) wr -> finalizeWriter wr lsm >>= \(wr, lsm) -> return (ws++wr, lsm))
				([], lsm)
				writers
			debugPutStrLn $ "Finalized writers:"
			forM_ finalWriters $ debugPutStrLn . ("    writer: "++) . show
			let	runs = reverse $ map writerToRun finalWriters
				kdWriter = last finalWriters
				level = makeLevel runs (not mergeNoDeletes) kdWriter
			return (level, lsm)
		finalizeWriter wr lsm = do
			let	wr' = wr { wrBuffer = mappend (wrBuffer wr) (writeByte 0) }	-- sequential runs are like these. end with key len 0.
				(lsm', wr'') = allocateForWrite True wr' lsm
			debugPutStrLn $ "Finalized writer "++show wr++"\n     wr'': "++show wr''
			(lsm'', wr''') <- actualWriterFlush True lsm' wr''
			return ([wr'''], lsm')
		allocateForWrite lastBlock writer lsm
			-- not enough space in allocated blocks.
			| pagesToAllocate > 0 = (lsmAllocated, writerAllocated)
			-- we have enough space in blocks we already allocated.
			| otherwise = (lsm, writer)
			where
				lsmAllocated = lsm {
					  lsmBlockCounts = Map.insert maxUsedPage (1, ourBlock) $ lsmBlockCounts lsm
					}
				writerAllocated = writer {
					  wrBlocks = wrBlocks writer ++ [ourBlock]
					}
				ourBlock = LSMBlock { lsmbAddr = maxUsedPage, lsmbSize = pagesToAllocate }
				usedBlocks = lsmBlockCounts lsm
				maxUsedPage = maybe 0 (\((_c,b),_) -> lsmbAddr b + lsmbSize b) $ Map.maxView usedBlocks
				filledPages = wrAbsPageIndex writer
				allocatedPages = sum $ map lsmbSize $ wrBlocks writer
				pagesUnwritten = allocatedPages - filledPages
				pagesDiff = pagesNeedToBeWritten - pagesUnwritten
				pagesToAllocate
					| lastBlock = pagesDiff
					| otherwise = max (wrAllocPagesCount writer) pagesDiff
				bytesNeedToBeWritten = fromIntegral (builderLength $ wrBuffer writer)
				pagesNeedToBeWritten = lsmBytesPages lsm bytesNeedToBeWritten
		flushWriter :: Bool -> (LSM, [Writer]) -> Writer -> IO (LSM, [Writer])
		flushWriter final (lsm, ws) writer = do
			debugPutStrLn $ "flushing "++show (final, writer)
			(lsm', writer') <- if flush
				then actualWriterFlush final lsm writer
				else return (lsm, writer)
			return (lsm', ws++[writer'])
			where
				flush = final || fromIntegral (builderLength buf) > lsmFlushThreshold lsm
				buf = wrBuffer writer
		actualWriterFlush final lsm writer
			| overflow && pagesRemain > 0 = do
				if bufPages >= pagesRemain
					then do
						debugPutStrLn $ "writer page start: "++show (writerPageStart writer)
						debugPutStrLn $ "writer: "++show writer
						lsmPutBuilderAtLogicalPage lsm (writerPageStart writer) pagesRemain (wrBuffer writer)
						let	wr = writer {
								  wrAbsPageIndex = pagesRemain + wrAbsPageIndex writer
								, wrBuffer = mempty
								}
						return (lsm, wr)
					else ned "bufPages < pagesRemain."
			| overflow = do
				ned $ "allocate and call again, pagesRemain "++show pagesRemain++"\nwriter "++show writer
			| otherwise = return (lsm, writer)
			where
				overflow = bufPages > pagesRemain || final
				bufPages = writerPagesInBuffer lsm writer
				pagesRemain = writerPagesRemains writer
		writeWriters :: LSM -> BS.ByteString -> Maybe BS.ByteString -> [Writer] -> IO ([Writer], LSM)
		writeWriters lsm key mbVal (wr:wrs) = do
			writers' <- go False mbVal wr wrs
			(lsm', writers'') <- foldM (flushWriter False) (lsm, []) writers'
			return (writers'', lsm')
			where
				keyLen = fromIntegral $ BS.length key
				writeIndexSeq mbValue wr = do
					let	value = Maybe.fromMaybe (internal "nothing for index seq?") mbValue
						add = mconcat [encodeLength 0 0 keyLen, encodeLength 0 0 (fromIntegral $ BS.length value), lazyByteString key, lazyByteString value]
					debugPutStrLn $ "index add: "++show add
					return $ wr {
						  wrBuffer = mappend (wrBuffer wr) add
						}
				writeKeyDataSeq mbValue wr = do
					let	specialFlag = Maybe.fromMaybe True $ fmap BS.null mbValue
						deleteFlag = not mergeNoDeletes && Maybe.isNothing mbValue
						addToLen = (if specialFlag then 1 else 0) + (if deleteFlag then 2 else 0)
						shift = if mergeNoDeletes then 1 else 2
						dataLen = fromIntegral $ maybe 0 BS.length mbValue
						dataEnc = Maybe.fromMaybe mempty $ fmap (\v -> if BS.null v then mempty else lazyByteString v) mbVal
						dataLenEnc = if dataLen > 0 then encodeLength 0 0 dataLen else mempty
						keyLenEnc = encodeLength shift addToLen keyLen
					return $ wr {
						  wrBuffer = mconcat [wrBuffer wr, keyLenEnc, dataLenEnc, lazyByteString key, dataEnc]
						, wrKeysWritten = let kw' = wrKeysWritten wr + 1 in
							if kw' >= branching then kw' - branching else kw'
						, wrKeysSize = fromIntegral (BS.length key) + wrKeysSize wr
						, wrDataSize = dataLen + wrDataSize wr
						}
				go indexWr mbValue wr [] = do
					debugPutStrLn $ "go:"
					debugPutStrLn $ "    indexWr: "++show indexWr
					debugPutStrLn $ "    mbValue: "++show mbValue
					debugPutStrLn $ "    wr: "++show wr
					debugPutStrLn $ "    (iwr:iwrs): []"
					wr' <- (if indexWr then writeIndexSeq else writeKeyDataSeq)
						mbValue wr
					return [wr']
				go indexWr mbValue wr (iwr:iwrs) = do
					debugPutStrLn $ "go:"
					debugPutStrLn $ "    indexWr: "++show indexWr
					debugPutStrLn $ "    mbValue: "++show mbValue
					debugPutStrLn $ "    wr: "++show wr
					debugPutStrLn $ "    (iwr:iwrs): "++show (iwr:iwrs)
					let	nextData = Just $ encodePos wr
					wrs <- if wrKeysWrittenModBranch wr == 0
							then go True nextData iwr iwrs
							else return (iwr:iwrs)
					wr' <- if indexWr
						then writeIndexSeq mbValue wr
						else writeKeyDataSeq mbValue wr
					let	kwmb' = wrKeysWrittenModBranch wr' + 1
						kwmb = if kwmb' >= lsmBranching lsm then kwmb' - lsmBranching lsm else kwmb'
						wr = wr' {
						  wrKeysWrittenModBranch	= kwmb
						}
					return $ wr : wrs
		encodePos wr = toLazyByteString $
			mconcat $ map encodeWideInt [absIndex + bufPages, ofsInPage ]
			where
				absIndex = wrAbsPageIndex wr
				bufSize = fromIntegral $ builderLength $ wrBuffer wr
				bufPages = shiftR bufSize shift
				shift = lsmhPageShift $ lsmiHeader $ lsmInfo lsm
				ofsInPage = bufSize .&. (shiftL 1 shift - 1)
		putIter n iter prioQ = do
			readResult <- readIterValue lsm iter
			case readResult of
				Just ((key, value), iter') -> case maybePrevValue of
					Just (oldn, oldv, olditer)
						-- the value in priority queue was more recent. we'll reread and add again.
						-- priority queue was not changed, actually, so we use old copy.
						| oldn < n -> putIter n iter' prioQ
						-- we have more recent value. the old iterator has to read and add.
						-- priority queue was changed, use new.
						| otherwise -> putIter oldn olditer prioQ'
					-- the key is absent in priority queue, success!
					Nothing -> return prioQ'
					where
						select _ new old@(oldn, oldMbVal, oldIter)
							| oldn < n = old
							| otherwise = new
						(maybePrevValue, prioQ') = Map.insertLookupWithKey select key (n,value,iter') prioQ
				Nothing -> return prioQ
		mergeNoDeletes = null rem
		readFirstValue map (n,iter) = do
			val <- readIterValue lsm iter
			debugPutStrLn $ "first value. iter: "++show iter
			debugPutStrLn $ "                n: "++show n
			debugPutStrLn $ "              val: "++show val
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
				writer = Writer {
					  wrAllocPagesCount		= allocPagesCount
					, wrAbsPageIndex		= 0
					, wrKeysWritten			= 0
					, wrKeysWrittenModBranch	= 0
					, wrKeysSize			= 0
					, wrDataSize			= 0
					, wrBlocks			= []
					, wrBuffer			= mempty
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
					computeMerge (null oldLevels) forceMergeAllNew (memCnt, (memKS+memDS), [IMem memMap]) newLevels
		computeMerge lastLevelLast forceAdd iters [] = (iters, [])
		computeMerge lastLevelLast forceAdd iters@(cnt, size, itersRev) rest@(level:levels)
			| add = computeMerge lastLevelLast forceAdd (cnt+lcnt, size+lsize, IDisk (DiskIter False hasDeletes True 0 0 16 mempty lblocks []) : itersRev) levels
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
		acq lsm b@(LSMBlock a s) = lsm'
			where
				lsm' = lsm {
					  lsmBlockCounts = Map.insertWith (\(c1,b') (c2,_) -> (c1+c2, b)) a (1,b) $ lsmBlockCounts lsm
					}
		rel lsm b@(LSMBlock a s)
			| Just (n,b') <- old, n > 1 = lsm { lsmBlockCounts = counts }
			| Just (n,b') <- old, n <= 1 = lsm { lsmBlockCounts = Map.delete a counts, lsmFreeBlocks = mergeFree (lsmFreeBlocks lsm) [b]}
			where
				(old, counts) = Map.insertLookupWithKey (\_ (o,b1) (c,_) -> (o+c,b1)) a (-1, b) $ lsmBlockCounts lsm

acquireReleaseLevels :: LSM -> [LSMLevel] -> [LSMLevel] -> LSM
acquireReleaseLevels lsm acquire release = acquireReleaseBlocks lsm (lsmLevelsBlocks acquire) (lsmLevelsBlocks release)

mergeFree [] released = released
mergeFree free [] = free
mergeFree a@(lb1:lb1s) b@(lb2:lb2s)
	| t1 < a2 = lb1 : mergeFree lb1s b
	| t2 < a1 = lb2 : mergeFree a lb2s
	| t1 == a2 = mergeFree (LSMBlock a1 (lsmbSize lb1 + lsmbSize lb2) : lb1s) lb2s
	| t2 == a1 = mergeFree (LSMBlock a2 (lsmbSize lb1 + lsmbSize lb2) : lb1s) lb2s
	| otherwise = internal $ "blocks are not disjoint or adjacent.\na: "++show a++"\nb: "++show b
	where
		a1 = lsmbAddr lb1
		a2 = lsmbAddr lb2
		t b = lsmbAddr b+lsmbSize b
		t1 = t lb1
		t2 = t lb2

replaceOldestState :: LSM -> [LSMLevel] -> IO LSM
replaceOldestState lsm levels = do
	debugPutStrLn $ "Oldest state: "++show oldest
	debugPutStrLn $ "new state: "++show new
	writeState True lsm new
	debugPutStrLn $ "resulting states: "++show (lsmiStates $ lsmInfo result)
	return result
	where
		result = lsm {
			  lsmInfo = let info = lsmInfo lsm in info {
				  lsmiStates = map toNew $ lsmiStates info
				}
			}
		toNew s
			| lsmsPhysIndex s == lsmsPhysIndex new = new
			| otherwise = s
		statesSeqI = map (\s -> (lsmsSeqIndex s, s)) $ lsmiStates $ lsmInfo lsm
		oldest = snd $ minimum statesSeqI
		mostRecentSeqIndex = fst $ maximum statesSeqI
		new = oldest {
			  lsmsSeqIndex = mostRecentSeqIndex + 1
			, lsmsLevels = levels
			}

startIterator :: LSMCmdChan -> WideInt -> WideInt -> Bool -> Bool -> LSMRun -> IO DiskIter
startIterator chan page offset hasDels hasSpecl run
	| page >= sum (map lsmbSize blocks) = internal $ "page "++show page++" is outside of blocks of run: "++show blocks
	| otherwise = do
	return $ DiskIter {
		  diDone		= False
		, diHasDeletes		= hasDels
		, diHasSpecial		= hasSpecl
		, diPageOffset		= offset
		, diPageIndex		= page
		, diPagesPreread	= 2	-- should be computed from level stats, actually, or from branching factor and distance in pages between positions in index.
		, diBuffer		= BS.empty
		, diBlocks		= blocks
		, diKVs			= []
		}
	where
		blocks = lsmrBlocks run

diskIterKeyLenShift :: DiskIter -> Int
diskIterKeyLenShift di
	| not $ diHasSpecial di = 0
	| not $ diHasDeletes di = 1
	| otherwise = 2

diskIterCurrentLogicalPageBlock :: DiskIter -> LSMBlock
diskIterCurrentLogicalPageBlock iter = findPageBlockAbs (diPageIndex iter) (diBlocks iter)

internalReadIter :: LSM -> DiskIter -> IO DiskIter
internalReadIter lsm iter
	-- Finished.
	| diDone iter = return iter
	-- has some key-value pairs.
	| not $ null (diKVs iter) = return iter
	| otherwise = readPairs iter
	where
		keyLenShift = diskIterKeyLenShift iter
		blocks = diBlocks iter
		builderBuf = diBuffer iter
		readDataIn pagesNeeded lsm iter = do
			debugPutStrLn $ "reading data for iter "++show iter
			debugPutStrLn $ "                      buffer before: "++show (diBuffer iter)
			let	LSMBlock addr size = diskIterCurrentLogicalPageBlock iter
				count = min size pagesNeeded
			debugPutStrLn $ "                      reading from addr: "++show addr
			debugPutStrLn $ "                              count: "++show count
			pages' <- readLogicalPages lsm addr count
			let	ofs = diPageOffset iter
				pages = if ofs > 0 then BS.drop (fromIntegral ofs) pages' else pages'
				iter' = iter {
					  diBuffer = BS.concat [diBuffer iter, pages]
					, diPageOffset = 0
					, diPageIndex = diPageIndex iter + count
					}
			debugPutStrLn $ "                      buffer after: "++show (diBuffer iter')
			return iter'
		parsePair buf
			-- too small even for key length.
			| not canReadKeyLen = Left 1
			-- has key length, and key length is zero (end of data).
			| keyLen' == 0 = Left 0
			-- has key length, but no data length.
			| not canReadDataLen = Left 1	-- TODO: convert key length to pages, maybe? experiment one day.
			| wholeOrdealSize > bufLen = Left $ lsmBytesPages lsm wholeOrdealSize
			-- has enough data for key length, data length, key and data.
			| otherwise = Right ((k,v), afterData)
			where
				bufLen = bystrLen buf
				keyLenBytes = decodeNeedBytes buf
				canReadKeyLen = bufLen >= keyLenBytes
				(keyLen', bufAfterKeyLen) = decodeWideInt buf
				dataLenBytes = decodeNeedBytes bufAfterKeyLen
				zeroDataLen = odd keyLen'
				deletedData = odd (shiftR keyLen' 1)
				keyLen = shiftR keyLen' keyLenShift
				wholeOrdealSize = keyLenBytes + dataLenBytes + keyLen + dataLen
				(k, afterKey) = BS.splitAt (fromIntegral keyLen) bufAfterDataLen
				(v, afterData)
					| diHasDeletes iter && deletedData = (Nothing, afterKey)
					| diHasSpecial iter && zeroDataLen = (Just BS.empty, afterKey)
					| otherwise = let (v,r) = BS.splitAt (fromIntegral dataLen) afterKey in (Just v, r)
				(canReadDataLen, (dataLen, bufAfterDataLen))
					| diHasSpecial iter && zeroDataLen = (True, (0, bufAfterKeyLen))
					| otherwise = (dataLenBytes <= bystrLen bufAfterKeyLen, decodeWideInt bufAfterKeyLen)
		parsePairs buf = case parsePair buf of
			Left pagesNeed -> Left pagesNeed
			ekvb -> let
					fromRight = either (internal "can't be Left") id
					kvbs = map fromRight $ takeWhile isRight (iterate (parsePair . snd . fromRight) ekvb)
					buf = snd $ last kvbs
				in Right (map fst kvbs, buf)
		readPairs iter = case parsePairs buf of
			Left pagesNeed
				| pagesNeed == 0  -> return iter {diDone = True}
				| otherwise -> do
					iter' <- readDataIn (max pagesNeed $ diPagesPreread iter) lsm iter
					readPairs iter'
			Right (kvs, buf') -> do
				return iter { diBuffer = buf', diKVs = kvs }
			where
				buf = diBuffer iter

navigateIterForKey :: Bool -> LSMCmdChan -> BS.ByteString -> DiskIter -> IO (Maybe (Maybe BS.ByteString), DiskIter)
navigateIterForKey lessOrEq cmdChan key diskIter
	| diDone diskIter = return (Nothing, diskIter)
	| null kvs = do
		result <- newEmptyMVar
		writeChan cmdChan $ ReadIter diskIter result
		diskIter <- takeMVar result
		navigateIterForKey lessOrEq cmdChan key diskIter
	| otherwise = case skipped of
		(Nothing, []) -> navigateIterForKey lessOrEq cmdChan key $ diskIter { diKVs = [] }
		(Nothing, kvs@((k,v) : kvs'))
			| lessOrEq -> return (Just v, diskIter { diKVs = kvs })
			| otherwise -> return (Nothing, diskIter { diKVs = kvs })
		(v, kvs) -> return (v, diskIter { diKVs = kvs })
	where
		kvs = diKVs diskIter
		skipped
			| lessOrEq = skipRemember Nothing key 1 kvs
			| otherwise = skip key 1 kvs

skip key n [] = (Nothing, [])
skip key 1 kvs@((k,v):kvs') = case compare key k of
	EQ -> (Just v, kvs')
	LT -> (Nothing, kvs)
	GT -> skip key 2 kvs'
skip key n kvs = case compare key upk of
	EQ -> (Just upv, rest)
	LT -> let (v',rest') = skip key 1 look in
		(v',rest'++rest)
	GT -> skip key (2*n) rest
	where
		up@(upk,upv) = last look
		(look, rest) = List.splitAt n kvs

skipRemember lt key n [] = (Nothing, Maybe.maybeToList lt)
skipRemember lt key 1 kvs@((k,v):kvs') = case compare key k of
	EQ -> (Just v, kvs')
	LT -> (Nothing, Maybe.maybeToList lt ++ kvs)
	GT -> skipRemember (Just (k,v)) key 2 kvs'
skipRemember lt key n kvs = case compare key upk of
	EQ -> (Just upv, rest)
	LT -> let (v',rest') = skipRemember lt key 1 look in
		(v',rest'++rest)
	GT -> skipRemember (Just up) key (2*n) rest
	where
		up@(upk,upv) = last look
		(look, rest) = List.splitAt n kvs


readInLevels :: LSMCmdChan -> BS.ByteString -> [LSMLevel] -> IO (Maybe BS.ByteString)
readInLevels chan key [] = return Nothing
readInLevels chan key (level:levels) = do
	v <- readInLevel 0 0 $ lsmlRuns level
	case v of
		Nothing -> readInLevels chan key levels
		x -> return x
	where
		hasDels = not $ null levels
		readInLevel page offset [keyDataRun] = do
			diskIter <- startIterator chan page offset hasDels True keyDataRun
			(mbValue, diskIter) <- navigateIterForKey False chan key diskIter
			debugPutStrLn $ "navigation in keyData run: "++show mbValue
			internalReleaseBlocks chan $ lsmrBlocks keyDataRun
			return $ Maybe.fromMaybe Nothing mbValue
		readInLevel page offset (keyPageOfsRun:kpors) = do
			diskIter <- startIterator chan page offset False False keyPageOfsRun
			(mbValue, diskIter) <- navigateIterForKey True chan key diskIter
			debugPutStrLn $ "navigation in pointers run: "++show mbValue
			internalReleaseBlocks chan $ lsmrBlocks keyPageOfsRun
			case mbValue of
				Just (Just pos) -> do
					let	(page, ofs) = evalState (liftM2 (,) decodeWideIntM decodeWideIntM) pos
					debugPutStrLn $ "Read position: "++show (pos, (page, ofs))
					readInLevel page ofs kpors
				Nothing -> return Nothing

-------------------------------------------------------------------------------
-- Worker loop.

lsmWorker :: LSM -> IO ()
lsmWorker lsm = do
	cmd <- readChan (lsmCmdChan lsm)
	case cmd of
		CopyLevels result -> do
			let	levelsToCopy = findRecentLevels lsm
				lsm' = acquireReleaseLevels lsm levelsToCopy []
			debugPutStrLn $ "levelsToCopy "++show levelsToCopy
			debugPutStrLn $ "lsmInfo "++show (lsmInfo lsm)
			putMVar result levelsToCopy
			lsmWorker lsm'
		Release blocks -> do
			let	blocksToRelease = Map.unions $ map (\b -> Map.singleton (lsmbAddr b) (1, lsmbSize b)) $ blocks
				blocksRemain = Map.mergeWithKey
					(\_a (cnt, b) (dec, size) -> let r = cnt - dec in if r < 1 then Nothing else Just (r, b))
					id
					(internal . ("blocks to free that are not in alloced blocks: " ++) . show)
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
						(lsm', mergedLevel, remainingLevels) <- mergeProcess lsm False (memCounts, memKeysSize, memDataSize, memoryPart) levels recentLevels
						let	lsm'' = acquireReleaseLevels lsm' (mergedLevel:remainingLevels) (levels ++ recentLevels)
						replaceOldestState lsm' (mergedLevel : remainingLevels)
				SnapshotIsolation
					-- no change.
					| memCounts == 0 && levels == recentLevels -> return lsm
					-- everything is on disk (can happen).
					| memCounts == 0 -> ned "SI: need to change levels!"
					-- performing merge.
					| otherwise -> ned "SI: need to merge with mem!"
			putMVar lock ()
			lsmWorker lsm'
		ReadIter diskIter result -> do
			diskIter' <- if null (diKVs diskIter)
				then do
					internalReadIter lsm diskIter
				else return diskIter
			putMVar result diskIter'
			lsmWorker lsm

-------------------------------------------------------------------------------
-- The external API.


-- |Create/open existing LSM file with specified parameters.
-- You probably should leave pageBits at 13 (8K bytes), it is good enough and
-- won't affect performance much.
-- The more states you keep in file (determined by statesCount), the more durable DB is.
--
newLSMWithConfig :: Int -> Int -> Bool -> FilePath -> IO LSMCmdChan
newLSMWithConfig pageBits statesCount forceCreate path
	| finiteBitSize (1::Int) < finiteBitSize (1::WideInt) = internal "need 64+-bit platform."
	| otherwise = do
	ch <- newChan
	lsm <- do
		h <- openBinaryFile path ReadWriteMode
		size <- hFileSize h
		let	info = emptyLSMInfo pageBits (fromIntegral statesCount)
			pages = fromIntegral $ div size (fromIntegral $ lsmInfoPageSize info)
			lsm = mkLSM info ch h pages
		if forceCreate
			then do
				writeHeader lsm
				writeStates lsm	-- this syncs. ;)
				return lsm
			else readHeaderInfo lsm
	debugPutStrLn $ "Worker will be spawn with info "++show (lsmInfo lsm)
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
			, lsmtiLevels		= internal "txni levels unset"
			, lsmtiMemThreshold	= defaultMemoryPartSize
			}


-- |Begin a nested transaction.
lsmNest :: LSMTxn -> IO LSMTxn
lsmNest txn = do
	ned "nested txn!"

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
	internalReleaseLevels (lsmtiChan txni) (lsmtiLevels txni)
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
				, lsmtiMemKeysSize = lsmtiMemKeysSize txni + fromIntegral (BS.length key)
				, lsmtiMemKeysCount = lsmtiMemKeysCount txni+1
				, lsmtiMemory = newMem
				}
	txni'' <- if lsmtiMemDataSize txni' + lsmtiMemKeysSize txni' >= lsmtiMemThreshold txni'
		then ned "flush in internal write!"
		else return txni'
	return (Just txni'', ())
	where
		vlen = fromIntegral . maybe 0 BS.length

lsmWrite :: LSMTxn -> BS.ByteString -> BS.ByteString -> IO ()
lsmWrite txn key value = lsmWriteInternal txn key (Just value)

lsmDelete :: LSMTxn -> BS.ByteString -> IO ()
lsmDelete txn key = lsmWriteInternal txn key Nothing

lsmRead :: LSMTxn -> BS.ByteString -> IO (Maybe BS.ByteString)
lsmRead txn key = withTxn "lsmRead" txn $ \txni -> do
	let	chan = lsmtiChan txni
	val <- case Map.lookup key $ lsmtiMemory txni of
		Just v -> return v
		Nothing -> do
			mbVal <- readInLevels chan key (lsmtiLevels txni)
			case (mbVal, lsmtiIsolation txni) of
				(Nothing, SnapshotIsolation) -> return Nothing
				(Nothing, ReadCommitted) -> do
					debugPutStrLn $ "Local txn levels "++show (lsmtiLevels txni)
					levels <- internalCopyLevels chan
					debugPutStrLn $ "Levels to read from "++show (levels)
					mbVal <- readInLevels chan key levels
					internalReleaseLevels chan levels
					return mbVal
				(x,_) -> return x
	return (Just txni, val)
