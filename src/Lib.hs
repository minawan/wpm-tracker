module Lib
    ( readAllWpm
    ) where

import Database.HDBC.Sqlite3 (connectSqlite3)
import Database.HDBC

convRow :: [SqlValue] -> String
convRow [sqlRowId, sqlWpm] = show rowId ++ ": " ++ show wpm
  where rowId = (fromSql sqlRowId)::Integer
        wpm = (fromSql sqlWpm)::Integer
convRow x = fail $ "Unexpected result: " ++ show x

readAllWpm :: String -> String -> IO ()
readAllWpm _ dbFilename = do
  conn <- connectSqlite3 dbFilename
  r <- quickQuery' conn
       "SELECT oid, wpm from stat ORDER BY oid" []
  let stringRows = map convRow r
  mapM_ putStrLn stringRows
  disconnect conn
