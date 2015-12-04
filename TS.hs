{-# OPTIONS_GHC -fno-warn-tabs #-}

module Main where

import Control.Monad

import qualified Data.ByteString.Lazy as BS

import S

file = "aaa.lsm"

mkBS :: String -> BS.ByteString
mkBS = BS.pack . map (fromIntegral . fromEnum)

testNewClose = do
	lsm <- newLSM True file
	lsmClose lsm

testWriteReadRollback = do
	lsm <- newLSM True file
	tx <- lsmBegin ReadCommitted lsm
	let	k = mkBS "a"
		v = mkBS "b"
	lsmWrite tx k v
	v' <- lsmRead tx k
	when (v' /= Just v) $ error $ "expected " ++ show (Just v) ++", got "++show v'
	lsmRollback tx
	lsmClose lsm

testWriteCommitReadRollback = do
	lsm <- newLSM True file
	tx <- lsmBegin ReadCommitted lsm
	let	k = mkBS "a"
		v = mkBS "b"
	lsmWrite tx k v
	lsmCommit tx
	tx <- lsmBegin ReadCommitted lsm
	v' <- lsmRead tx k
	when (v' /= Just v) $ error $ "expected " ++ show (Just v) ++", got "++show v'
	lsmRollback tx
	lsmClose lsm

main = do
	testWriteCommitReadRollback
	testWriteReadRollback
	--testNewClose

t = main
