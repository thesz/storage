{-# OPTIONS_GHC -fno-warn-tabs #-}

module Main where

import Control.Monad

import qualified Data.ByteString.Lazy as BS

import S

file = "aaa.lsm"

mkBS :: String -> BS.ByteString
mkBS = BS.pack . map (fromIntegral . fromEnum)

done = putStrLn "DONE"

testNewClose = do
	lsm <- newLSM True file
	lsmClose lsm
	done

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
	done

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
	done

testWriteCommitCloseOpenReadRollback = do
	lsm <- newLSM True file
	tx <- lsmBegin ReadCommitted lsm
	let	k = mkBS "a"
		v = mkBS "b"
	lsmWrite tx k v
	lsmCommit tx
	lsmClose lsm
	lsm <- newLSM False file
	tx <- lsmBegin ReadCommitted lsm
	v' <- lsmRead tx k
	when (v' /= Just v) $ error $ "expected " ++ show (Just v) ++", got "++show v'
	lsmRollback tx
	lsmClose lsm
	done

main = do
	testWriteCommitCloseOpenReadRollback
	testWriteCommitReadRollback
	testWriteReadRollback
	--testNewClose
	putStrLn "\n\nALL DONE"

t = main
