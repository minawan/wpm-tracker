module Main where

import Control.Monad (when)
import Database.HDBC.Sqlite3 (connectSqlite3)
import Database.HDBC
import System.Environment

import Lib

selectRowsFromStatTable :: String
selectRowsFromStatTable = "SELECT oid, * from stat ORDER BY oid"

main :: IO ()
main = do
  args <- getArgs
  let csvFilename = args !! 0
  let dbFilename = args !! 1
  conn <- connectSqlite3 dbFilename
  queryResult <- quickQuery' conn selectRowsFromStatTable []

  let rows = case sequence $ readAllRows queryResult of
               Just rows -> rows
               Nothing -> []
  when (null rows) $ putStrLn ("No stat records found in " ++ dbFilename)
  mapM_ putStrLn $ validateStatEntries rows 
  mapM_ (putStrLn . show) rows
  disconnect conn
