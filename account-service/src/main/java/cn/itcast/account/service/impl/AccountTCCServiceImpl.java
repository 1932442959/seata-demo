package cn.itcast.account.service.impl;

import cn.itcast.account.entity.AccountFreeze;
import cn.itcast.account.mapper.AccountFreezeMapper;
import cn.itcast.account.mapper.AccountMapper;
import cn.itcast.account.service.AccountTCCSerivce;
import io.seata.core.context.RootContext;
import io.seata.rm.tcc.api.BusinessActionContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service
public class AccountTCCServiceImpl implements AccountTCCSerivce {

    @Resource
    private AccountMapper accountMapper;

    @Resource
    private AccountFreezeMapper freezeMapper;

    @Override
    public void deduct(String userId, int money) {
        // 获取事物id
        String xid = RootContext.getXID();
        // 判断freeze中是否有冻结记录，如果有，一定是cancel执行过，需要拒绝业务
        AccountFreeze oldFreeze = freezeMapper.selectById(xid);
        if (oldFreeze != null) {
            return;
        }
        // 扣减可用余额
        accountMapper.deduct(userId, money);
        // 记录冻结金额 事物状态
        AccountFreeze freeze = new AccountFreeze();
        freeze.setUserId(userId);
        freeze.setFreezeMoney(money);
        freeze.setState(AccountFreeze.State.TRY);
        freeze.setXid(xid);
        freezeMapper.insert(freeze);
    }

    @Override
    public boolean confirm(BusinessActionContext ctx) {
        // 获取事物id
        String xid = ctx.getXid();
        // 根据id删除冻结记录
        int count = freezeMapper.deleteById(xid);
        return count == 1;
    }

    @Override
    public boolean cancel(BusinessActionContext ctx) {
        // 查询冻结记录
        String xid = ctx.getXid();
        String userId = ctx.getActionContext("userId").toString();
        AccountFreeze freeze = freezeMapper.selectById(xid);
        // 空回滚的判断，判断freeze是否为null，为null证明try没执行，需要空回滚
        if (freeze == null) {
            // 证明try没执行，需要空回滚，记录回滚状态
            freeze = new AccountFreeze();
            freeze.setUserId(userId);
            freeze.setFreezeMoney(0);
            freeze.setState(AccountFreeze.State.CANCEL);
            freeze.setXid(xid);
            freezeMapper.insert(freeze);
            return true;
        }
        // 幂等判断
        if (freeze.getState() == AccountFreeze.State.CANCEL) {
            // 已经处理过一次cancel了，无需重复处理
            return true;
        }
        // 恢复可用金额
        accountMapper.refund(freeze.getUserId(), freeze.getFreezeMoney());
        // 将冻结金额清零，状态改为 cancel
        freeze.setFreezeMoney(0);
        freeze.setState(AccountFreeze.State.CANCEL);
        // 更新冻结状态
        int count = freezeMapper.updateById(freeze);
        return count == 1;
    }
}
