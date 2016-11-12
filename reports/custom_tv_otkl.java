public void fillReport( con, filter, bitel.billing.server.reports.BGCSVReport.ReportResult result )
{
  query = " SELECT contract.title as "title", "
  query += " (SELECT par_1.val FROM contract_parameter_type_1_log par_1 WHERE par_1.cid=contract.id GROUP BY contract.id) as "fio", "
  query += " (SELECT par_2.address FROM contract_parameter_type_2 par_2 WHERE par_2.cid=contract.id and pid=20 GROUP BY contract.id) as "adress" "
  query += " FROM contract WHERE status=1 "
  query += " LIMIT " + ((pageIndex - 1) * pageSize )+ "," + pageSize;
 
  ps = con.prepareStatement( query );
  data = new ArrayList( 1000 );
 
  rs = ps.executeQuery();
 
  while( rs. next() )
  {
    title = rs.getString("title");
    fio = rs.getString("fio");
    adress = rs.getString("adress");
   
    map = new HashMap();
    map.put( "colum1", title );
    map.put( "colum2", fio );
    map.put( "colum3", adress );
    data.add( map );    
  }
 
  result.setData( data );
}
