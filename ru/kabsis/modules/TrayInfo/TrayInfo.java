package ru.kabsis.modules.TrayInfo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Date;

import bitel.billing.server.contract.bean.ContractPattern;
import bitel.billing.server.contract.bean.BalanceUtils;

import ru.bitel.bgbilling.modules.trayinfo.server.bean.TrayInfoReplyBuilder;
import ru.bitel.bgbilling.server.util.Setup;
import ru.bitel.common.Utils;
import bitel.billing.server.contract.bean.ContractManager;

public class TrayInfo
	implements TrayInfoReplyBuilder
{
        private Connection conSlave = null;

        @Override
        public void init( Setup setup, Connection conSlave, int mid )
        {
                this.conSlave = conSlave;
        }

        @Override
        public String reply( int cid )
        {
                ContractManager cm = new ContractManager(conSlave);
                String title = cm.getContractById(cid).getTitle();
                BalanceUtils cu = new BalanceUtils( conSlave );
        BigDecimal balance = cu.getBalance( new Date(), cid );
                return "Договор № "+title+" Баланс "+Utils.formatCost( balance )+" руб.";
        }
}