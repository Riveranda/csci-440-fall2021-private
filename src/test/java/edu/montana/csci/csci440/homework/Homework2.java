package edu.montana.csci.csci440.homework;

import edu.montana.csci.csci440.DBTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Homework2 extends DBTest {


    @Test
    public void problem1() {
        List<Map<String, Object>> results = executeSQL(
                "SELECT employees.FirstName, employees.LastName, employees.Title, COUNT(*) " +
                        "from employees " +
                        "INNER JOIN customers ON employees.EmployeeId=customers.SupportRepId; " +
                        "WHERE FirstName LIKE 'Steve' " +
                        "AND LastName LIKE 'Jobs'"
        );

    }

    @Test
    /*
     * Create a view tracksPlus to display the artist, song title, album, and genre for all tracks.
     */
    public void createTracksPlusView() {
        executeDDL(
                "CREATE VIEW tracksPlus AS " +
                        "SELECT genres.Name as GenreName, artists.Name as ArtistName, albums.Title as AlbumTitle, tr.TrackId " +
                        "FROM tracks as tr " +
                        "INNER JOIN albums ON tr.AlbumId = albums.AlbumId " +
                        "INNER JOIN artists ON albums.ArtistId = artists.ArtistId " +
                        "INNER JOIN genres ON tr.GenreId = genres.GenreId "

        );

        List<Map<String, Object>> results = executeSQL("SELECT * FROM tracksPlus ORDER BY TrackId");
        assertEquals(3503, results.size());
        assertEquals("Rock", results.get(0).get("GenreName"));
        assertEquals("AC/DC", results.get(0).get("ArtistName"));
        assertEquals("For Those About To Rock We Salute You", results.get(0).get("AlbumTitle"));
    }

    @Test
    /*
     * Create a table grammy_infos to track grammy information for an artist.  The table should include
     * a reference to the artist, the album (if the grammy was for an album) and the song (if the grammy was
     * for a song).  There should be a string column indicating if the artist was nominated or won.  Finally,
     * there should be a reference to the grammy_category table
     *
     * Create a table grammy_category
     */
    public void createGrammyInfoTable() {
        executeDDL("create table grammy_categories (" +
                "GrammyCategoryId INTEGER NOT NULL PRIMARY KEY, " +
                "Name varchar(255) " +
                ");"
        );

        executeDDL("create table grammy_infos (" +
                "Status varchar(15)," +
                "ArtistId INTEGER ," +
                "AlbumId INTEGER," +
                "TrackId INTEGER," +
                "GrammyCategoryId INTEGER," +
                "FOREIGN KEY (ArtistId) REFERENCES artists (ArtistId)," +
                "FOREIGN KEY (AlbumId) REFERENCES albums(AlbumId), " +
                "FOREIGN KEY (TrackId) REFERENCES tracks(TrackId)," +
                "FOREIGN KEY (GrammyCategoryId) REFERENCES grammy_categories(GrammyCategoryId)" +
                ");"
        );

        // TEST CODE
        executeUpdate("INSERT INTO grammy_categories(Name) VALUES ('Greatest Ever');");
        Object categoryId = executeSQL("SELECT GrammyCategoryId FROM grammy_categories").get(0).get("GrammyCategoryId");

        executeUpdate("INSERT INTO grammy_infos(ArtistId, AlbumId, TrackId, GrammyCategoryId, Status) VALUES (1, 1, 1, " + categoryId + ",'Won');");

        List<Map<String, Object>> results = executeSQL("SELECT * FROM grammy_infos");
        assertEquals(1, results.size());
        assertEquals(1, results.get(0).get("ArtistId"));
        assertEquals(1, results.get(0).get("AlbumId"));
        assertEquals(1, results.get(0).get("TrackId"));
        assertEquals(1, results.get(0).get("GrammyCategoryId"));
    }

    @Test
    /*
     * Bulk insert five categories of your choosing in the genres table
     */
    public void bulkInsertGenres() {
        Integer before = (Integer) executeSQL("SELECT COUNT(*) as COUNT FROM genres").get(0).get("COUNT");

        executeUpdate("INSERT INTO genres(Name) " +
                "VALUES " +
                "(\"Smooth Jazz\")," +
                "(\"KPop\")," +
                "(\"Bongo Drums\")," +
                "(\"Himalayan Screeching\")," +
                "(\"EarBleed\");"
        );

        Integer after = (Integer) executeSQL("SELECT COUNT(*) as COUNT FROM genres").get(0).get("COUNT");
        assertEquals(before + 5, after);
    }

}
