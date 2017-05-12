package extension;

import java.util.*;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import static org.neo4j.helpers.collection.MapUtil.stringMap;


public class FullTextIndex
{
    // Only static fields and @Context-annotated fields are allowed in
    // Procedure classes. This static field is the configuration we use
    // to create full-text indexes.
    private static final Map<String,String> FULL_TEXT =
            stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" );

    private static final Map<String, String> FULL_INDEX_CONFIG =
            stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext", "analyzer", "org.wltea.analyzer.lucene.IKAnalyzer");

    public static final String NODE = "NODE";
    public static final String RELATIONSHIP = "RELATIONSHIP";
    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;


    // TODO: This is here as a workaround, because index().forNodes() is not read-only
    @Procedure(value = "example.search", mode = Mode.WRITE)
    @Description("Execute lucene query in the given index, return found nodes")
    public Stream<SearchHit> search( @Name("label") String label,
                                     @Name("query") String query )
    {
        String index = indexName( label );

        // Avoid creating the index, if it's not there we won't be
        // finding anything anyway!
        if( !db.index().existsForNodes( index ))
        {
            // Just to show how you'd do logging
            log.debug( "Skipping index query since index does not exist: `%s`", index );
            return Stream.empty();
        }

        // If there is an index, do a lookup and convert the result
        // to our output record.
        return db.index()
                .forNodes( index )
                .query( query )
                .stream()
                .map( SearchHit::new );
    }

    @Procedure(value = "userdefined.index.ChineseFullIndexSearch", mode = Mode.WRITE)
    @Description("call userdefined.index.ChineseFullIndexSearch(indexName, query, limit) yield node, 执行lucene全文搜索，返回前 {limit} 个结果")
    public Stream<ChineseHit> searchchinese(@Name("indexName") String indexName,
                                            @Name("query") String query,
                                            @Name("limit") long limit
    ){
        if( !db.index().existsForNodes( indexName ))
        {
            // Just to show how you'd do logging
            log.debug( "Skipping index query since index does not exist: `%s`", indexName );
            return Stream.empty();
        }

        return db.index()
                .forNodes(indexName, FULL_INDEX_CONFIG)
                .query(new QueryContext(query).sortByScore().top((int)limit))
                .stream()
                .map(ChineseHit::new);
    }


    @Procedure(value = "example.index", mode=Mode.WRITE)
    @Description("For the node with the given node-id, add properties for the provided keys to index per label")
    public void index( @Name("nodeId") long nodeId,
                       @Name("properties") List<String> propKeys )
    {
        Node node = db.getNodeById(nodeId);
        // Load all properties for the node once and in bulk,
        // the resulting set will only contain those properties in `propKeys`
        // that the node actually contains.


        Set<Map.Entry<String,Object>> properties =
                node.getProperties( propKeys.toArray( new String[0] ) ).entrySet();

        // Index every label (this is just as an example, we could filter which labels to index)
        for ( Label label : node.getLabels() )
        {
            Index<Node> index = db.index().forNodes( indexName( label.name() ), FULL_TEXT );

            // In case the node is indexed before, remove all occurrences of it so
            // we don't get old or duplicated data
            index.remove( node );

            // And then index all the properties
            for ( Map.Entry<String,Object> property : properties )
            {
                index.add( node, property.getKey(), property.getValue() );
            }
        }
    }





    @Procedure(value = "userdefined.index.addChineseFullTextIndex", mode=Mode.WRITE)
    @Description("For the node with the given node-id, add properties for the provided keys to index per label")
    public void addIndex( @Name("indexName") String indexName,
                          @Name("labelName") String labelName,
                          @Name("properties") List<String> propKeys )
    {
        Label label = Label.label(labelName);
        ResourceIterator<Node> nodes = db.findNodes(label);
        // Load all properties for the node once and in bulk,
        // the resulting set will only contain those properties in `propKeys`
        // that the node actually contains.


        while(nodes.hasNext()){
            Node node = nodes.next();
            Set<Map.Entry<String,Object>> properties =
                    node.getProperties( propKeys.toArray( new String[0] ) ).entrySet();

            // Index every label (this is just as an example, we could filter which labels to index)

            Index<Node> index = db.index().forNodes( indexName, FULL_INDEX_CONFIG);

            // In case the node is indexed before, remove all occurrences of it so
            // we don't get old or duplicated data
            index.remove( node );

            // And then index all the properties
            for ( Map.Entry<String,Object> property : properties )
            {
                index.add( node, property.getKey(), property.getValue() );
            }

        }

    }

    @Procedure(value = "chineseFulltextIndex.queryByValue", mode = Mode.WRITE)
    public Stream<NodeAndScore> queryByValue(@Name("value")String value){

        IndexManager mgr = db.index();
        String[] indexes = mgr.nodeIndexNames();
        return queryByLabel(Arrays.asList(indexes), value);
    }

    @Procedure(value = "chineseFulltextIndex.queryByLabel", mode = Mode.WRITE)
    public Stream<NodeAndScore> queryByLabel(@Name("labels") List<String> labels, @Name("value") String value){
        IndexManager mgr = db.index();
        Stream<NodeAndScore> resultStream = Stream.empty();
        for(String index: labels){
            Iterable<String> propKeys = db.findNodes(Label.label(index)).next().getPropertyKeys();
            List<String> listPropKeys = new ArrayList<>();
            for(String propKey: propKeys){
                listPropKeys.add(propKey);
            }
            Stream<NodeAndScore> aResult = queryByProperty(index, listPropKeys, value);
            resultStream = Stream.concat(resultStream, aResult);
        }
        return resultStream.sorted(Comparator.comparing(NodeAndScore::getScore).reversed());
    }


    @Procedure(value = "chineseFulltextIndex.queryByProperty", mode = Mode.WRITE)
    public Stream<NodeAndScore> queryByProperty(@Name("label") String label, @Name("propKeys") List<String> propKeys, @Name("value") String value){
        IndexManager mgr = db.index();
        StringBuilder query = new StringBuilder();
        for(String propKey: propKeys){
            query.append(propKey + ":" + value + " ");
            query.append("OR ");
        }
        String queryc = query.substring(0, query.length()-4);
        Index<Node> fulltextIndex = mgr.forNodes(label);
        IndexHits<Node> result = fulltextIndex.query(new QueryContext(queryc));
        Stream<NodeAndScore> aResult = result
                .map(res -> new NodeAndScore(res, (double)result.currentScore()))
                .stream();
        return  aResult;
    }

    @Procedure(value = "chineseFulltextIndex.addNodesIndexByLabels", mode = Mode.WRITE)
    public void addNodesIndexByLabels(@Name("labels")List<String> labels){
        for(String label:labels){
            addNodesIndexByLabel(label);
        }
    }

    @Procedure(value = "chineseFulltextIndex.addNodesIndex")
    public void addNodesIndex(){
        List<String> labels = new ArrayList<>();
        ResourceIterable getLabels = db.getAllLabels();
        for(Object label:getLabels){
            labels.add(label.toString());
        }
        addNodesIndexByLabels(labels);
    }

    @Procedure(value = "chineseFulltextIndex.addNodeIndexByLabel", mode = Mode.WRITE)
    public void addNodesIndexByLabel(@Name("label")String label){
//        IndexManager mgr = db.index();
//        if(mgr.existsForNodes(label)){
//            mgr.forNodes(label).delete();
//        }
//        Index<Node> index= mgr.forNodes(label, FULL_INDEX_CONFIG);
//        db.findNodes(Label.label(label)).stream().peek(
//                node -> addNodeIndex(node, index)
//        );
        Label labelName = Label.label(label);
        ResourceIterator<Node> nodes = db.findNodes(labelName);

        while(nodes.hasNext()){
            Node node = nodes.next();
            Set<Map.Entry<String,Object>> properties =
                    node.getAllProperties().entrySet();

            Index<Node> index = db.index().forNodes( label, FULL_INDEX_CONFIG);

            index.remove( node );

            for ( Map.Entry<String,Object> property : properties )
            {
                index.add( node, property.getKey(), property.getValue() );
            }

        }
    }

    public void addNodeIndex(Node node, Index<Node> index){
        Set<Map.Entry<String,Object>> properties =
                node.getProperties().entrySet();

        for ( Map.Entry<String,Object> property : properties )
        {
            index.add( node, property.getKey(), property.getValue() );
        }
    }

    @Procedure(value = "chineseFulltextIndex.removeIndex", mode = Mode.WRITE)
    public Stream<IndexInfo> removeIndex(){
        IndexManager mgr = db.index();
        String[] indexNames = mgr.nodeIndexNames();
        List<IndexInfo> indexInfos = new ArrayList<>();
        for(String indexname:indexNames){
            Index<Node> index = mgr.forNodes(indexname);
            indexInfos.add(new IndexInfo(NODE, indexname, mgr.getConfiguration(index)));
            index.delete();
        }
        return indexInfos.stream();
    }

    @Procedure(value = "chineseFulltextIndex.removeIndexByLabel", mode = Mode.WRITE)
    public Stream<IndexInfo> removeIndexByLabel(@Name("name") String name) {
        IndexManager mgr = db.index();
        List<IndexInfo> indexInfos = new ArrayList<>();
        if (mgr.existsForNodes(name)) {
            Index<Node> index = mgr.forNodes(name);
            indexInfos.add(new IndexInfo(NODE, name, mgr.getConfiguration(index)));
            index.delete();
        }
        return indexInfos.stream();
    }

    public static class IndexInfo {
        public final String type;
        public final String name;
        public final Map<String,String> config;

        public IndexInfo(String type, String name, Map<String, String> config) {
            this.type = type;
            this.name = name;
            this.config = config;
        }
    }


    public static class NodeAndScore{
        public final Node node;
        public final double score;

        public Node getNode() {
            return node;
        }

        public double getScore() {
            return score;
        }

        public NodeAndScore(Node node, double score){
            this.node = node;
            this.score = score;

        }
    }

    public static class SearchHit
    {
        // This records contain a single field named 'nodeId'
        public long nodeId;

        public SearchHit( Node node )
        {
            this.nodeId = node.getId();
        }
    }

    public static class ChineseHit
    {
        public Node node;

        public ChineseHit(Node node) {this.node = node;}
    }



    private String indexName( String label )
    {
        return label;
    }

}