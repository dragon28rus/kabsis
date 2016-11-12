public void fillReport( con, filter, bitel.billing.server.reports.BGCSVReport.ReportResult result )
{
  query = " SELECT * from contract ";
  query += " LIMIT " + ((pageIndex - 1) * pageSize )+ "," + pageSize;
 
  ps = con.prepareStatement( query );
  data = new ArrayList( 1000 );
 
  rs = ps.executeQuery();
 
  while( rs. next() )
  {
    title = rs.getString("title");
    comment = rs.getString("comment");
   
    map = new HashMap();
    map.put( "title", title );
    map.put( "comment", comment );
    data.add( map );    
  }
 
  result.setData( data );
}
