module Lib
    ( readAllWpm
    ) where

import Data.List (intercalate)
import Database.HDBC.Sqlite3 (connectSqlite3)
import Database.HDBC

type RowId = Integer
type Date = String
type Wpm = Integer
type High = Integer

data StatEntry = StatEntry RowId Date Wpm High

instance Show StatEntry where
  show (StatEntry rowId date wpm high) = intercalate "," [show rowId, date, show wpm, show high]

convRow :: [SqlValue] -> Maybe StatEntry
convRow [sqlRowId, sqlDate, sqlWpm, sqlHigh] = Just $ StatEntry rowId date wpm high
  where rowId = (fromSql sqlRowId)::Integer
        date = (fromSql sqlDate)::String
        wpm = (fromSql sqlWpm)::Integer
        high = (fromSql sqlHigh)::Integer
convRow _ = Nothing

readAllWpm :: String -> String -> IO ()
readAllWpm _ dbFilename = do
  conn <- connectSqlite3 dbFilename
  r <- quickQuery' conn
       "SELECT oid, * from stat ORDER BY oid" []
  case sequence $ map convRow r of
    Just rows -> mapM_ (putStrLn . show) rows
    Nothing -> return ()
  disconnect conn
