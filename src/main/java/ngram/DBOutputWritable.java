package ngram;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable {
    // we store n-gram to predict 1-gram
    private String starting_phrase;
    private String following_word;
    private int count;

    public DBOutputWritable(String starting_phrase, String following_word, int count) {
        this.starting_phrase = starting_phrase;
        this.following_word = following_word;
        this.count = count;
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.starting_phrase);
        statement.setString(2,this.following_word);
        statement.setInt(3,this.count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.starting_phrase = resultSet.getString(1);
        this.following_word = resultSet.getString(2);
        this.count = resultSet.getInt(3);
    }
}
