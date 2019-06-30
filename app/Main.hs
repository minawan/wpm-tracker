module Main where

import Control.Monad (when)
import qualified Data.ByteString.Lazy as BL (readFile)
import Data.Csv (decodeByName)
import qualified Data.Vector as Vector (toList)
import Database.HDBC.Sqlite3 (connectSqlite3)
import Database.HDBC (disconnect, quickQuery')
import System.Environment (getArgs)
import Text.Printf (printf)

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

  stat <- BL.readFile csvFilename
  let records = case decodeByName stat of
                      Left err -> fail err
                      Right (_, records) -> Vector.toList records
  mapM_ putStrLn $ validateStatEntries records
  when (records /= rows) $ putStrLn "Stat table inconsistent with DB records."

  disconnect conn
