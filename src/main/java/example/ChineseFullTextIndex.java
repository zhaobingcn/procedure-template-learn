package example;

import jdk.nashorn.internal.runtime.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by hexu on 2016/12/28.
 */
public class ChineseFullTextIndex {

    /**
     * lucene config
     */
    private static final Map<String, String> FULL_INDEX_CONFIG =
            MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext", "analyzer", "org.wltea.analyzer.lucene.IKAnalyzer");

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "userdefined.index.ChineseFullIndexSearch", mode = Mode.WRITE)
    @Description("执行lucene全文搜索，返回前 {limit} 个结果")
    public Stream<Node> search(@Name("indexName") String indexName,
                               @Name("query") String query,
                               @Name("limit") int limit
    ){
        if( !db.index().existsForNodes( indexName ))
        {
            // Just to show how you'd do logging
            log.debug( "Skipping index query since index does not exist: `%s`", indexName );
            return Stream.empty();
        }

        return db.index()
                .forNodes(indexName, FULL_INDEX_CONFIG)
                .query(new QueryContext(query).sortByScore().top(limit))
                .stream();
    }

    @Procedure(value = "userdefined.index.AddChineseFullIndex", mode = Mode.WRITE)
    @Description("添加lucene全文索引")
    public void index(@Name("nodeId") long nodeId,
                      @Name("properties") List<String> properties
    ){

    }

}
