<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
    "http://www.w3.org/TR/html4/loose.dtd">
<%@ page import="com.accumed.pricing.PricingEngine" %>
<%@ page import="com.accumed.pricing.LoggingManager" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Santechture UAE Pricing Engine</title>
        <style>
            body{
                font-size: 10pt;
            }
            #main{
                width: 100%;
                text-align: center;
            }
            #titleContainer{
                font: bold 25pt monospace;
                color: midnightblue;
            }
            .peFieldSet{
                padding-left: 10px;
                margin-right: 15px;
                margin-left: 5px;
                margin-bottom: 10px;
            }
            legend{
                color: midnightblue;
                font: bold 12pt monospace;
            }
            .peTabDiv{
                padding-left: 50px;
                padding-right: 25px;
            }
            .peLabelInTable{
                text-align: left;
            }
            .peValueInTable{
                text-align: left;
            }
            .peSepInTable{
                text-align: left;
                width:50px;
            }
        </style>
    </head>
    <body>
        <form action="${pageContext.request.contextPath}/jspActionsServlet" method="post">
            <%

                java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("dd,MMM yyyy hh:mm:ss");
                com.accumed.pricing.cachedRepo.CachedRepository repo = null;
                com.accumed.pricing.AccountantPool accountantPool = null;
                int accountantsCount = 0;
                int asynchronousAccountants = 0;
                int synchronousAccountants = 0;
                int busyAccountantsCount = 0;
                String accountantLaws = "";
                String accountantLawsLoadingTime = "";
                java.util.HashMap<String, Long> tables = null;
                java.util.List<String> loadedCustomeContracts = null;
                if (com.accumed.pricing.PricingEngine.getCachedRepositoryService() != null
                        && com.accumed.pricing.PricingEngine.getCachedRepositoryService().getRepo() != null
                        && com.accumed.pricing.PricingEngine.getCachedRepositoryService().getRepo().getCachedDB() != null) {
                    repo = com.accumed.pricing.PricingEngine.getCachedRepositoryService().getRepo();
                    tables = repo.getDistinctTableList();
                    loadedCustomeContracts = repo.getLoadedCustomeContracts();

                    if (com.accumed.pricing.PricingEngine.getAccountantPool() != null) {
                        accountantPool = com.accumed.pricing.PricingEngine.getAccountantPool();
                        if (accountantPool != null) {
                            accountantsCount = accountantPool.getCount();
                            asynchronousAccountants = accountantPool.getAsynchronizedCount();
                            synchronousAccountants = accountantPool.getSynchronizedCount();
                            busyAccountantsCount = accountantPool.getCheckedoutCount();
                            accountantLaws = accountantPool.getRulesPackage();
                            accountantLawsLoadingTime = formatter.format(new java.util.Date(accountantPool.getRulesPackageTime()));
                        }
                    }
                }

                String sMinRequest = "0";
                if (PricingEngine.getMinRequest() != null) {
                    sMinRequest = PricingEngine.getMinRequest().getPeriodInMilli() + "/APM-" + PricingEngine.getMinRequest().getClaimId();
                }

                String sMaxRequest = "0";
                if (PricingEngine.getMaxRequest() != null) {
                    sMaxRequest = PricingEngine.getMaxRequest().getPeriodInMilli() + "/APM-" + PricingEngine.getMaxRequest().getClaimId();
                }

                String sLastMinuteRequestCount = "0";
                String sLastMinuteAverageRequestTime = "0";
                if (PricingEngine.getPricingLogger() != null) {
                    com.accumed.pricing.Pair<Long, Long> pair = PricingEngine.getPricingLogger().getLastMinuteAverageRequestTime(1);
                    if (pair.getElement0() > 0) {
                        sLastMinuteAverageRequestTime = (pair.getElement1() / pair.getElement0()) + "";
                        sLastMinuteRequestCount = pair.getElement0() + "";
                    }
                }

                String sLast10MinuteRequestCount = "0";
                String sLast10MinuteAverageRequestTime = "0";
                if (PricingEngine.getPricingLogger() != null) {
                    com.accumed.pricing.Pair<Long, Long> pair = PricingEngine.getPricingLogger().getLastMinuteAverageRequestTime(10);
                    if (pair.getElement0() > 0) {
                        sLast10MinuteAverageRequestTime = (pair.getElement1() / pair.getElement0()) + "";
                        sLast10MinuteRequestCount = pair.getElement0() + "";
                    }
                }

                String sLast30MinuteRequestCount = "0";
                String sLast30MinuteAverageRequestTime = "0";
                if (PricingEngine.getPricingLogger() != null) {
                    com.accumed.pricing.Pair<Long, Long> pair = PricingEngine.getPricingLogger().getLastMinuteAverageRequestTime(30);
                    if (pair.getElement0() > 0) {
                        sLast30MinuteAverageRequestTime = (pair.getElement1() / pair.getElement0()) + "";
                        sLast30MinuteRequestCount = pair.getElement0() + "";
                    }
                }

                boolean isCachedRepositoryAgentRunning = false;
                boolean isDroolsUpdaterAgentRunning = false;
                boolean isPricingLoggerAgentRunning = false;
                isCachedRepositoryAgentRunning = com.accumed.pricing.cachedRepo.BackgroundTaskManager.isRunningCachedRepositoryFuture();
                isDroolsUpdaterAgentRunning = com.accumed.pricing.cachedRepo.BackgroundTaskManager.isRunningDroolsUpdaterFuture();
                isPricingLoggerAgentRunning = com.accumed.pricing.cachedRepo.BackgroundTaskManager.isRunningPricingLoggerFuture();

                LoggingManager loggingManager = LoggingManager.getInstance();
                boolean logInfoEnabled = loggingManager.isLogInfoEnabled();
                boolean logRequest = loggingManager.isLogRequest();

            %>

            <table id="main">
                <tr><td><div id="titleContainer"><label id="title">Santechture UAE Pricing Engine</label></div></td></tr>
            </table>
            <hr style="height:5px; background-color: black;"/>
            <%if (repo != null && accountantPool != null) {%>
            <fieldset class="peFieldSet" title="Cached Repository">
                <legend>Cached Repository</legend>
                <table>
                    <tr>
                        <td>
                            <table class="peTabDiv">
                                <tr><td class="peLabelInTable">Status:</td><td class="peValueInTable"><%=repo.isValid() ? "valid" : "<span style='color:red;'>invalid</span>"%></td><td class="peSepInTable"></td></tr>
                                <tr><td class="peLabelInTable">Cashed tables count:</td><td class="peValueInTable"><%=repo.getDistinctTableList().size()%></td></tr>
                                <tr><td class="peLabelInTable">Invalid objects count:</td><td class="peValueInTable"><%=repo.getInvalidCachedDataCount() == 0 ? "0" : "<span style='color:red;'>" + repo.getInvalidCachedDataCount() + "</span>"%></td></tr>
                                <tr><td class="peLabelInTable">Timestamp:</td><td class="peValueInTable"><%=formatter.format(repo.getTimeStamp())%></td><td class="peSepInTable"></td></tr>
                            </table>
                        </td>
                        <td>
                            <div style="height:150px; width:250px;overflow: auto;">
                                <p>Cached Tables</p>
                                <table>
                                    <% if (tables != null) {
                                            int cnt = 0;

                                            for (String sKey : tables.keySet()) {
                                                //entry.getValue()
%>
                                    <tr><td><%=++cnt%></td><td><%=sKey%></td></tr>
                                    <%}
                                        }%>
                                </table>
                            </div>

                        </td>
                        <td>
                            <div style="height:150px; width:250px;overflow: auto;">
                                <p>Loaded custom contracts</p>
                                <table>
                                    <% if (loadedCustomeContracts != null) {
                                            int cnt = 0;

                                            for (String fac : loadedCustomeContracts) {
                                                //entry.getValue()
%>
                                    <tr><td><%=++cnt%></td><td><%=fac%></td></tr>
                                    <%}
                                        }%>
                                </table>
                            </div>

                        </td>
                    </tr>
                </table>
            </fieldset>
            <fieldset class="peFieldSet" title="Pricing Accountants">
                <legend>Pricing Accountants</legend>
                <table class="peTabDiv">
                    <tr><td class="peLabelInTable">Accountants Count</td><td class="peValueInTable"><%=accountantsCount%></td><td class="peSepInTable"></td><td class="peLabelInTable">Synchronous Accountants</td><td class="peValueInTable"><%=synchronousAccountants%></td><td class="peSepInTable"></td><td class="peLabelInTable">Accountant Laws</td><td class="peValueInTable"><%=accountantLaws%></td></tr>
                    <tr><td class="peLabelInTable">Busy Accountants</td><td class="peValueInTable"><%=busyAccountantsCount%></td><td class="peSepInTable"></td><td class="peLabelInTable">Asynchronous Accountants</td><td class="peValueInTable"><%=asynchronousAccountants == 0 ? ("" + asynchronousAccountants) : ("<span style='color:red;'>" + asynchronousAccountants + "</span>")%></td><td class="peSepInTable"></td><td class="peLabelInTable">Laws versions</td><td class="peValueInTable"><%=accountantLawsLoadingTime%></td></tr>
                </table>
            </fieldset>
            <fieldset class="peFieldSet" title="Service Agents">
                <legend>Service Agents</legend>
                <table class="peTabDiv">
                    <tr><td class="peLabelInTable">Cached Repository Agent</td><td class="peValueInTable"><%=isCachedRepositoryAgentRunning ? "running" : "<span style='color:red;'>stopped</span>"%></td><td class="peSepInTable"></td><td class="peLabelInTable"><input  type="submit" name="RestartAllAgents" value="Restart All"/></td></tr>
                    <tr><td class="peLabelInTable">Laws Agent</td><td class="peValueInTable"><%=isDroolsUpdaterAgentRunning ? "running" : "<span style='color:red;'>stopped</span>"%></td><td class="peSepInTable"></td><td class="peLabelInTable">--</td></tr>
                    <tr><td class="peLabelInTable">Logger Agent</td><td class="peValueInTable"><%=isPricingLoggerAgentRunning ? "running" : "<span style='color:red;'>stopped</span>"%></td><td class="peSepInTable"></td><td class="peLabelInTable">--</td></tr>
                </table>
            </fieldset>

            <fieldset class="peFieldSet" title="Pricing Service" style=" margin-bottom: 0px;">
                <legend>Pricing Service</legend>
                <table class="peTabDiv">
                    <tr><td class="peLabelInTable">--</td><td class="peValueInTable">--</td><td class=nTable"></td><td class="peLabelInTable">Last request time:</td><td class="peValueInTable"><%=PricingEngine.getLastRequestTime() == null ? "0" : PricingEngine.getLastRequestTime()%></td></tr>
                    <tr><td class="peLabelInTable">Average pricing time:</td><td class="peValueInTable"><%=PricingEngine.getAverageClaimPricingTime() == null ? "0" : PricingEngine.getAverageClaimPricingTime()%></td><td class=nTable"></td><td class="peLabelInTable">Last minute priced claims count:</td><td class="peValueInTable"><%=sLastMinuteRequestCount%></td></tr>
                    <tr><td class="peLabelInTable">Min. pricing time:</td><td class="peValueInTable"><%=sMinRequest%></td><td class="peSepInTable"></td><td class="peLabelInTable">Last 10 minutes priced claims count:</td><td class="peValueInTable"><%=sLast10MinuteRequestCount%></td></tr>
                    <tr><td class="peLabelInTable">Max. pricing time:</td><td class="peValueInTable"><%=sMaxRequest%></td><td class=nTable"></td><td class="peLabelInTable">Last 30 minutes priced claims count:</td><td class="peValueInTable"><%=sLast30MinuteRequestCount%></td></tr>
                    <tr><td class="peLabelInTable">Last minute average time:</td><td class="peValueInTable"><%=sLastMinuteAverageRequestTime%></td><td class="peSepInTable"></td><td class="peLabelInTable">Total requests:</td><td class="peValueInTable"><%=PricingEngine.getTotalRequests()%></td></tr>
                    <tr><td class="peLabelInTable">Last 10 minutes average time:</td><td class="peValueInTable"><%=sLast10MinuteAverageRequestTime%></td><td class="peSepInTable"></td><td class="peLabelInTable">Total Priced claims count:</td><td class="peValueInTable"><%=PricingEngine.getTotalPricedClaims()%></td></tr>
                </table>
            </fieldset>
            <table id="footer" style="width:100%;">
                <tr><td style=" text-align: right;"><%@ include file="version.jsp" %></td></tr>
            </table>
            <%} else {%>
            <div style="font-size:35pt; color: red; width: 100%; height: 100%; text-align: center; vertical-align: middle;">Initializing ...</div>
            <%}%>
        </form>
        <%if (repo != null && accountantPool != null) {%>
        <form action="toggleLogging.jsp" method="post">
            <input type="hidden" name="action1" value="<%= logInfoEnabled ? "disable" : "enable"%>">

            <button type="submit">
                <%= logInfoEnabled ? "Disable Rules Logging" : "Enable Rules Logging"%>
            </button>
            :<label><%= logInfoEnabled ? "Rules Logging Is Enabled" : "Rules Logging Is Disabled"%></label><br/>

        </form>
        <%}%>
        <%if (repo != null && accountantPool != null) {%>
        <form action="toggleLogging.jsp" method="post">
            <input type="hidden" name="action2" value="<%= logRequest ? "disable" : "enable"%>">
            <button type="submit">
                <%= logRequest ? "Disable Request Logging" : "Enable Request Logging"%>
            </button>
            :<label><%= logRequest ? "Request Logging Is Enabled" : "Request Logging Is Disabled"%></label>
        </form>
        <%}%>
    </body>
</html>
