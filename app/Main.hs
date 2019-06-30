{-# LANGUAGE DeriveDataTypeable #-}

module Main where

import Control.Monad (when)
import qualified Data.ByteString.Lazy as BL (readFile)
import Data.Csv (decodeByName)
import qualified Data.Vector as Vector (toList)
import Database.HDBC.Sqlite3 (connectSqlite3)
import Database.HDBC (disconnect, quickQuery')
import System.Console.CmdArgs (Data, Typeable, cmdArgs, help, summary, (&=))
import Text.Printf (printf)

import Lib

selectRowsFromStatTable :: String
selectRowsFromStatTable = "SELECT oid, * from stat ORDER BY oid"

data Flag = Flag { stat_file :: String
                 , db_file :: String
                 }
  deriving (Show, Data, Typeable)

flag = Flag { stat_file = "stat.txt" &= help "Stat file to validate"
            , db_file = "stat.sqlite3" &= help "Stat database file to load"
            } &= summary "Stat Validator"

main :: IO ()
main = do
  flags <- cmdArgs flag
  let csvFilename = stat_file flags
  let dbFilename = db_file flags
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
