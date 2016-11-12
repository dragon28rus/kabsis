package ru.kabsis;

import bitel.billing.server.contract.bean.ContractManager;
import ru.bitel.bgbilling.kernel.container.managed.ServerContext;
import ru.bitel.bgbilling.kernel.contract.api.common.event.ContractModifiedEvent;
import ru.bitel.bgbilling.kernel.contract.api.common.service.ContractService;
import ru.bitel.bgbilling.kernel.event.Event;
import ru.bitel.bgbilling.kernel.module.common.bean.User;
import ru.bitel.bgbilling.kernel.script.server.dev.EventScriptBase;
import ru.bitel.bgbilling.server.util.Setup;
import ru.bitel.common.sql.ConnectionSet;
import ru.bitel.common.Utils;

public class GroupRemove
	extends EventScriptBase
{
    private final static long GROUP_REMOVE = 12;
	@Override
	public void onEvent( Event event, Setup setup, ConnectionSet set )
		throws Exception
	{
		print("test");
	    int contractId = event.getContractId();
	    ServerContext context = ServerContext.get( ServerContext.class );
	    ContractManager cm = new ContractManager( set.getConnection() );
	    cm.deleteContractGroup( contractId, GROUP_REMOVE );
	    context.publishAfterCommit( new ContractModifiedEvent( User.USER_SERVER, contractId ) );
	    
	}

} 
