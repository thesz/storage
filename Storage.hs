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

import qualified Data.ByteString as BS

import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as VU

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
                ByteArr         -- the byte array of left subtree.
                (PT a)          -- left subtree.
                (PT a)          -- right subtree.
        deriving (Eq, Ord, Show)



--------------------------------------------------------------------------------
-- The storage.

