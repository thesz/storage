-- |Storage.hs
--
-- An ACID storage implementation.
--
-- Copyright (C) 2013 Serguey Zefirov.

module Storage where

import Control.Monad
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.Chan

import Data.Bits
import qualified Data.ByteString as BS

import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as VU
import Data.Word

import System.IO

--------------------------------------------------------------------------------
-- Types for storage.

type ByteVec = VU.Vector Word8

type Key = ByteVec
type Value = ByteVec

--------------------------------------------------------------------------------
-- Patricia tree for fast lookup and priority tree construction.
-- 

data PT a =
        -- trivial empty constructor.
                PTNull
        -- also trivial singleton constructor.
        |       PTSingle ByteVec a
        -- split constructor.
        -- left and right subtrees are equal up to point of split, recorded in first two arguments.
        |       PTSplit
                Int             -- split point: index into arrays.
                Int             -- split point: bit index (zero is highest bit)
                ByteVec         -- the byte array of left subtree.
                (PT a)          -- left subtree.
                (PT a)          -- right subtree.
        deriving (Eq, Ord, Show)

ptEmpty :: PT a
ptEmpty = PTNull

ptSingleton :: ByteVec -> a -> PT a
ptSingleton = PTSingle

findSplit :: Int -> ByteVec -> ByteVec -> (Int, Int)
findSplit pos a b
        | pos >= VU.length a = (pos, 0)
        | pos >= VU.length b = (pos, 0)
        | aByte == bByte = findSplit (pos+1) a b
        | otherwise = (pos, splitBit 0 (xor aByte bByte))
        where
                aByte = a VU.! pos
                bByte = b VU.! pos
                splitBit n byte
                        | testBit byte (7-n) = n
                        | otherwise = splitBit (n+1) byte


ptInsert :: ByteVec -> a -> PT a -> PT a
ptInsert key value PTNull = PTSingle key value
ptInsert key value tree = go 0 tree
        where
                getKey (PTSingle key value) = key
                getKey (PTSplit _ _ key _ _) = key
                go prevSplitPos tree@(PTSingle key' value')
                        -- check if equal.
                        | splitIndex >= VU.length key && splitIndex >= VU.length key' = PTSingle key value
                        -- key is less than key' - equal prefi, but shorter.
                        | splitIndex >= VU.length key = PTSplit splitIndex 0 key (PTSingle key value) tree
                        | splitIndex >= VU.length key' = PTSplit splitIndex 0 key' tree (PTSingle key value)
                        | key VU.! splitIndex < key' VU.! splitIndex =
                                PTSplit splitIndex splitBit key (PTSingle key value) tree
                        | otherwise = PTSplit splitIndex splitBit key' tree (PTSingle key value)
                        where
                                (splitIndex, splitBit) = findSplit prevSplitPos key key'
                go prevSplitPos tree@(PTSplit thisSplitPos thisSplitBit key' l r)
                        -- new split is after the current split - key is less than current PTSplit key.
                        | splitIndex > thisSplitPos || (splitIndex == thisSplitPos && splitBit > thisSplitBit) =
                                let l' = go thisSplitPos l
                                in PTSplit thisSplitPos thisSplitBit (getKey l') l r
                        -- new split if before current split - key is ушерук ыьфддук щк bigger than фдд луны шт ЗЕЫздше ыгиекуую.
                        | splitIndex < thisSplitPos || (splitIndex == thisSplitPos && splitBit < thisSplitBit) =
                                if splitIndex >= VU.length key || keyByte < key'Byte
                                        then PTSplit splitIndex splitBit key (PTSingle key value) tree
                                        else PTSplit splitIndex splitBit key' tree (PTSingle key value)
                        -- we have split at exactly the same point. The key goes to right.
                        | otherwise = PTSplit splitIndex splitBit key' l (go splitIndex r)
                        where
                                keyByte = key VU.! splitIndex
                                key'Byte = key VU.! splitIndex
                                (splitIndex, splitBit) = findSplit prevSplitPos key key'

ptToList :: PT a -> [(ByteVec, a)]
ptToList PTNull = []
ptToList (PTSingle key value) = [(key, value)]
ptToList (PTSplit _ _ _ l r) = ptToList l ++ ptToList r

byteVecFromList :: [Word8] -> ByteVec
byteVecFromList bytes = VU.fromList bytes

--------------------------------------------------------------------------------
-- The storage.
--
-- All sizes are Integer's, because native speed of Integer operations for
-- small integers are about same as for

type Address = Integer

minimalPageSize :: Integer
minimalPageSize = 256

data Storage = Storage {
          storageHandle         :: Handle
        , storageMemSize        :: Address
        , storagePageSize       :: Address
        , storageHeaderPages    :: Address
        , storageHeadersCount   :: Address
        , storageHeaders        :: MVar [Header]
        }

data Header = Header {
          headerSeqIndex        :: Address
        , headerPrevSeqIndex    :: Address
        , headerPhysiIndex      :: Address
        , headerRuns            :: [Run]
        }
        deriving Show

-- |Run contains sizes of data stored (count, keys size, data size) and hierarchy of extent sequences.
data Run = Run Address Address Address [[Extent]]
        deriving Show

-- |Sequence of pages.
data Extent = Extent Address Address
        deriving Show

readHeader :: Handle -> IO Storage
readHeader handle = do
        hSeek handle AbsoluteSeek 0
        error "readHeader!!!"

storageOpen :: FilePath -> IO Storage
storageOpen fn = do
        h <- openBinaryFile fn ReadWriteMode
        readHeader h
