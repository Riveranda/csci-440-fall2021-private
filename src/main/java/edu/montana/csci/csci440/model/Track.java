package edu.montana.csci.csci440.model;

import edu.montana.csci.csci440.util.DB;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Track extends Model {

    private Long trackId;
    private Long albumId;
    private Long mediaTypeId;
    private Long genreId;
    private String name;
    private Long milliseconds;
    private Long bytes;
    private BigDecimal unitPrice;
    private String albumName;
    private String artistName;

    public static final String REDIS_CACHE_KEY = "cs440-tracks-count-cache";

    public Track() {
        mediaTypeId = 1l;
        genreId = 1l;
        milliseconds = 0l;
        bytes = 0l;
        unitPrice = new BigDecimal("0");
    }

    protected Track(ResultSet results) throws SQLException {
        name = results.getString("Name");
        milliseconds = results.getLong("Milliseconds");
        bytes = results.getLong("Bytes");
        unitPrice = results.getBigDecimal("UnitPrice");
        trackId = results.getLong("TrackId");
        albumId = results.getLong("AlbumId");
        mediaTypeId = results.getLong("MediaTypeId");
        genreId = results.getLong("GenreId");
        albumName = getAlbum().getTitle(); //cached these here
        artistName = getAlbum().getArtist().getName();
    }

    public static Track find(long i) {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM tracks WHERE TrackId=?")) {
            stmt.setLong(1, i);
            ResultSet results = stmt.executeQuery();
            if (results.next()) {
                return new Track(results);
            } else {
                return null;
            }
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static Long count() {
        Jedis redisClient = new Jedis();// use this class to access redis and create a cache
        if (redisClient.exists(REDIS_CACHE_KEY)) {
            return Long.parseLong(redisClient.get(REDIS_CACHE_KEY));
        }

        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) as Count FROM tracks")) {
            ResultSet results = stmt.executeQuery();
            if (results.next()) {
                long c = results.getLong("Count");
                redisClient.set(REDIS_CACHE_KEY, String.valueOf(results.getLong("Count")));
                return c;
            } else {
                throw new IllegalStateException("Should find a count!");
            }
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public Album getAlbum() {
        return Album.find(albumId);
    }

    public MediaType getMediaType() {
        return null;
    }

    public Genre getGenre() {
        return null;
    }

    public List<Playlist> getPlaylists() {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT playlist_track.PlaylistId, Name " +
                             "FROM playlist_track " +
                             "JOIN playlists ON " +
                             "playlists.PlaylistId = playlist_track.PlaylistId " +
                             "WHERE TrackId = ?")) {
            stmt.setLong(1, trackId);
            ResultSet results = stmt.executeQuery();
            List<Playlist> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Playlist(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public Long getTrackId() {
        return trackId;
    }

    public void setTrackId(Long trackId) {
        this.trackId = trackId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getMilliseconds() {
        return milliseconds;
    }

    public void setMilliseconds(Long milliseconds) {
        this.milliseconds = milliseconds;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public Long getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Long albumId) {
        this.albumId = albumId;
    }

    public void setAlbum(Album album) {
        albumId = album.getAlbumId();
    }

    public Long getMediaTypeId() {
        return mediaTypeId;
    }

    public void setMediaTypeId(Long mediaTypeId) {
        this.mediaTypeId = mediaTypeId;
    }

    public Long getGenreId() {
        return genreId;
    }

    public void setGenreId(Long genreId) {
        this.genreId = genreId;
    }

    public String getArtistName() {
        return this.artistName;
    }

    public String getAlbumTitle() {
        return this.albumName;
    }

    public static List<Track> advancedSearch(int page, int count,
                                             String search, Integer artistId, Integer albumId,
                                             Integer maxRuntime, Integer minRuntime) {
        LinkedList<Object> args = new LinkedList<>();

        String query = "SELECT ";

        if (maxRuntime != null) {
            query += "/*+ MAX_EXECUTION_TIME(" + maxRuntime + ") */";
        }

        query += "* FROM tracks " +
                "JOIN albums ON tracks.AlbumId = albums.AlbumId " +
                "WHERE name LIKE ?";
        args.add("%" + search + "%");

        // Conditionally include the query and argument
        if (artistId != null) {
            query += " AND ArtistId=? ";
            args.add(artistId);
        }
        if (albumId != null) {
            query += " AND AlbumId=? ";
            args.add(albumId);
        }
        query += " LIMIT ? OFFSET ?";
        args.add(count);
        args.add(count * (page - 1));

        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                stmt.setObject(i + 1, arg);
            }
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> search(int page, int count, String orderBy, String search) {
        String query = "SELECT * FROM tracks ORDER BY " + orderBy + " LIKE ? LIMIT ? OFFSET ?";
        search = "%" + search + "%";
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, search);
            stmt.setInt(2, count);
            stmt.setInt(3, (page - 1) * count);
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> forAlbum(Long albumId) {
        String query = "SELECT * FROM tracks WHERE AlbumId=?";
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, albumId);
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    // Sure would be nice if java supported default parameter values
    public static List<Track> all() {
        return all(0, Integer.MAX_VALUE);
    }

    public static List<Track> all(int page, int count) {
        return all(page, count, "TrackId");
    }

    public static List<Track> all(int page, int count, String OrderBy) {
        String query = "SELECT tracks.*, albums.Title as AlbumTitle, artists.Name as ArtistName " +
                "    FROM tracks " +
                "    JOIN albums on tracks.AlbumId = albums.AlbumId " +
                "    JOIN artists on albums.ArtistId = artists.ArtistId " +
                "    ORDER BY " + OrderBy + " " +
                "    LIMIT ? OFFSET ?";

        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setInt(1, count);
            stmt.setInt(2, count * (page - 1));
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new ArrayList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    @Override
    public boolean create() {
        Jedis jedisClient = new Jedis();
        if (verify()) {
            try (Connection conn = DB.connect();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO tracks (Name, AlbumId, MediaTypeId, GenreId," +
                                 " Milliseconds, Bytes, UnitPrice) VALUES (?, ?, ?, ?, ?, ?, ?)"
                 )) {
                stmt.setString(1, name);
                stmt.setLong(2, albumId);
                stmt.setLong(3, mediaTypeId);
                stmt.setLong(4, genreId);
                stmt.setLong(5, milliseconds);
                stmt.setLong(6, bytes);
                stmt.setBigDecimal(7, unitPrice);
                int result = stmt.executeUpdate();
                trackId = DB.getLastID(conn);
                jedisClient.del(REDIS_CACHE_KEY);
                return result == 1;
            } catch (SQLException sqlException) {
                throw new RuntimeException(sqlException);
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean verify() {
        _errors.clear();
        if (name == null || name.equals("")) {
            addError("Name cannot be null");
        }
        if (albumId == null || albumId == 0) {
            addError("AlbumId cannot be null");
        }
        return !hasErrors();
    }

    @Override
    public void delete() {
        Jedis jedis = new Jedis();
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "DELETE FROM tracks WHERE trackid = ?"
             )) {
            stmt.setLong(1, trackId);
            stmt.executeUpdate();
            jedis.del(REDIS_CACHE_KEY);
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    @Override
    public boolean update() {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE tracks SET Name=?, AlbumId=?, MediaTypeId=?, GenreId=?," +
                             " Milliseconds=?, Bytes=?, UnitPrice=? WHERE TrackId=?"
             )) {
            stmt.setString(1, name);
            stmt.setLong(2, albumId);
            stmt.setLong(3, mediaTypeId);
            stmt.setLong(4, genreId);
            stmt.setLong(5, milliseconds);
            stmt.setLong(6, bytes);
            stmt.setBigDecimal(7, unitPrice);
            stmt.setLong(8, trackId);
            int result = stmt.executeUpdate();
            return result == 1;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }
}
