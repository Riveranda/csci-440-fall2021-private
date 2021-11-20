package edu.montana.csci.csci440.model;

import edu.montana.csci.csci440.util.DB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Album extends Model {

    Long albumId;
    Long artistId;
    String title;

    public Album() {
    }

    private Album(ResultSet results) throws SQLException {
        title = results.getString("Title");
        albumId = results.getLong("AlbumId");
        artistId = results.getLong("ArtistId");
    }

    public Artist getArtist() {
        return Artist.find(artistId);
    }

    public void setArtist(Artist artist) {
        artistId = artist.getArtistId();
    }

    public List<Track> getTracks() {
        return Track.forAlbum(albumId);
    }

    public Long getAlbumId() {
        return albumId;
    }

    public void setAlbum(Album album) {
        this.albumId = album.getAlbumId();
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String name) {
        this.title = name;
    }

    public Long getArtistId() {
        return artistId;
    }

    public static List<Album> all() {
        return all(0, Integer.MAX_VALUE);
    }

    public static List<Album> all(int page, int count) {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT * FROM albums LIMIT ? OFFSET ?"
             )) {
            stmt.setInt(1, count);
            stmt.setInt(2, count * (page - 1));
            ResultSet results = stmt.executeQuery();
            List<Album> resultList = new ArrayList<>();
            while (results.next()) {
                resultList.add(new Album(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    @Override
    public boolean create() {
        if(verify()) {
            try (Connection conn = DB.connect();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO albums (Title, ArtistId) VALUES (?, ?)"
                 )) {
                stmt.setString(1, title);
                stmt.setLong(2, artistId);
                int result = stmt.executeUpdate();
                albumId = DB.getLastID(conn);
                return result == 1;
            } catch (SQLException sqlException) {
                throw new RuntimeException(sqlException);
            }
        }
        else {
            return false;
        }
    }

    @Override
    public boolean verify() {
        _errors.clear();
        if(artistId == null || artistId == 0)
            addError("Null AlbumId");
        if(title == null || title.equals(""))
            addError("Null Title");
        return !hasErrors();
    }

    @Override
    public boolean update() {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE albums SET Title=?, ArtistId=? WHERE AlbumId=?"
             )) {
            stmt.setString(1, title);
            stmt.setLong(2, artistId);
            stmt.setLong(3, albumId);
            int result = stmt.executeUpdate();
            return result == 1;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static Album find(long i) {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM albums WHERE AlbumId=?")) {
            stmt.setLong(1, i);
            ResultSet results = stmt.executeQuery();
            if (results.next()) {
                return new Album(results);
            } else {
                return null;
            }
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Album> getForArtist(Long artistId) {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM albums WHERE ArtistId=?")) {
            stmt.setLong(1, artistId);
            ResultSet results = stmt.executeQuery();
            List<Album> albums = new LinkedList<>();
            while (results.next()) {
                albums.add( new Album(results));
            }
            return albums;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

}
