{-# OPTIONS_GHC -fno-warn-tabs #-}

module Main where

import Control.Monad

import qualified Data.ByteString.Lazy as BS

import Data.Time.Clock

import S

file = "aaa.lsm"

mkBS :: String -> BS.ByteString
mkBS = BS.pack . map (fromIntegral . fromEnum)

done = putStrLn "DONE"

runtime name act = do
	start <- getCurrentTime
	x <- act
	end <- getCurrentTime
	putStrLn $ show name++" done in "++show (diffUTCTime end start)
	return x

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
	when (v' /= Just v) $ error $ "expected " ++ show (Just v) ++", got "++show v'++"for key "++show k
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

testWriteUpTo16KCommitCloseOpenReadRollback = do
	forM (takeWhile (<=16384) $ iterate (*2) 2) $ test
	done
	where
		test n = do
			runtime ("wrote "++show n) $ do
				putStrLn $ "testing portion of size "++show n
				lsm <- newLSM True file
				putStrLn $ "Created."
				--getLine
				tx <- lsmBegin ReadCommitted lsm
				forM_ [1..n] $ \i -> do
					let	k = mkBS $ show i
						v = mkBS $ show (i+n)
					lsmWrite tx k v
				lsmCommit tx
				lsmClose lsm
				putStrLn $ "Wrote "++show n
			runtime ("read back "++show n) $ do
				--getLine
				lsm <- newLSM False file
				putStrLn "Reopened."
				tx <- lsmBegin ReadCommitted lsm
				forM_ [1..n] $ \i -> do
					let	k = mkBS $ show i
						v = mkBS $ show (i+n)
					v' <- lsmRead tx k
					when (v' /= Just v) $ error $ "expected " ++ show (Just v) ++", got "++show v'++" for key "++show k
				lsmRollback tx
				lsmClose lsm


main = do
	testWriteUpTo16KCommitCloseOpenReadRollback
	testWriteCommitCloseOpenReadRollback
	testWriteCommitReadRollback
	testWriteReadRollback
	--testNewClose
	putStrLn "\n\nALL DONE"

t = main
