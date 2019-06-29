module Lib
    ( readAllWpm
    ) where

import Database.HDBC.Sqlite3 (connectSqlite3)
import Database.HDBC

type RowId = Integer
type Date = String
type Wpm = Integer
type High = Integer

data StatEntry = StatEntry RowId Date Wpm High
  deriving (Show)

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
  let stringRows = map (show . convRow) r
  mapM_ putStrLn stringRows
  disconnect conn
