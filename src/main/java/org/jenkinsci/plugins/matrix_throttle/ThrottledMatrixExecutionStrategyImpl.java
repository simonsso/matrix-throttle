package org.jenkinsci.plugins.matrix_throttle;

import hudson.AbortException;
import hudson.Extension;
import hudson.Util;
import hudson.console.ModelHyperlinkNote;
import hudson.matrix.MatrixAggregator;
import hudson.matrix.MatrixChildAction;
import hudson.matrix.MatrixConfiguration;
import hudson.matrix.MatrixExecutionStrategy;
import hudson.matrix.MatrixExecutionStrategyDescriptor;
import hudson.matrix.MatrixRun;
import hudson.matrix.Messages;
import hudson.matrix.MatrixBuild;
import hudson.matrix.MatrixBuild.MatrixBuildExecution;
import hudson.model.Cause.UpstreamCause;
import hudson.model.Result;
import hudson.model.Run;
import hudson.util.FormValidation;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import javax.servlet.ServletException;

import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

/**
* {@link MatrixExecutionStrategy} that throttles matrix builds
*
* <p>
* Matrix build throttle strategy which limits the number of jobs submitted in build queue
*
* @author Fredrik Simonsson
* @since 1.502
*/

@Extension
public class ThrottledMatrixExecutionStrategyImpl extends MatrixExecutionStrategy {
	private final Integer maxParalellInstances;

@DataBoundConstructor
public ThrottledMatrixExecutionStrategyImpl(Integer maxParalellInstances){
	this.maxParalellInstances=maxParalellInstances;
}

public ThrottledMatrixExecutionStrategyImpl(){
	// Default value -- should not be used if configuration works.
	// value 0 will not throttle the number of jobs submitted to the queue
		maxParalellInstances=0;
	}
 /**
  * Filter to select a number of combinations to build first
  */

 @Override
 public Result run(MatrixBuildExecution execution) throws InterruptedException, IOException {
     PrintStream logger = execution.getListener().getLogger();

     int numberRunningParalellInstances=0;

     MatrixConfiguration pendingConfigurations[] =new MatrixConfiguration[maxParalellInstances];

     if (notifyStartBuild(execution.getAggregators())) return Result.FAILURE;
     Result r = Result.SUCCESS;

     for (MatrixConfiguration c: execution.getActiveConfigurations()){
         if(maxParalellInstances>0 && numberRunningParalellInstances < maxParalellInstances	){
        	 pendingConfigurations[numberRunningParalellInstances++]=c;
        	 scheduleConfigurationBuild(execution, c);
         }
         while( maxParalellInstances>0 && numberRunningParalellInstances >= maxParalellInstances	){
        	 // Max capacity wait to free resources
        	 boolean dirty=false;
        	 for(int i=0;i<maxParalellInstances;i++){
        		 MatrixConfiguration currentConfiguration=pendingConfigurations[i];
        		 if(currentConfiguration == null){
        			 // Null pointer in array repack later. 
        			 dirty=true;
        		 }else{
        			 boolean isPending=currentConfiguration.isInQueue();
        			 boolean isBuilding=currentConfiguration.isBuilding();
        			 
        			 if(!isBuilding&& !isPending){
        				 dirty=true;
        				 Result ans=getResult_retry(execution,currentConfiguration);
        			     logger.println(Messages.MatrixBuild_Completed(ModelHyperlinkNote.encodeTo(currentConfiguration),ans ));
        			     if(ans!=null){
        			    	 r=r.combine(ans);
        			     }
        			     
        				 pendingConfigurations[i]=null;
            			 notifyEndBuild(getMatrixRun(execution,currentConfiguration),execution.getAggregators());

        				 numberRunningParalellInstances--;
        			 }
        		 }
        	 }
        	 if(dirty){
        		 // jobs completed pack the array
        		 int j=0;
        		 for(int i=0;i<maxParalellInstances;i++){
        			 if(pendingConfigurations[i]!=null ){
        				 pendingConfigurations[j++]=pendingConfigurations[i];
        			 }
        		 }
        		 for(int i=j;i<maxParalellInstances;i++){
        			 pendingConfigurations[i]=null;
        		 }
        	 }
        	 Thread.sleep(1000);
         }
     }
     //All builds have been started now and at this point up to maxParalelInstances can still be running. Wait for them to complete
     while(numberRunningParalellInstances>0){
    	 for(int i=0;i<maxParalellInstances;i++){
    		 MatrixConfiguration currentConfiguration=pendingConfigurations[i];
    		 if(currentConfiguration != null){
    			 boolean isBuilding=currentConfiguration.isBuilding();
    			 boolean isPending=currentConfiguration.isInQueue();
    			 if(!isBuilding&& !isPending){
    				 Result ans=getResult_retry(execution,currentConfiguration);
    			     logger.println(Messages.MatrixBuild_Completed(ModelHyperlinkNote.encodeTo(currentConfiguration),ans ));
    			     if(ans!=null){
    			    	 r=r.combine(ans);
    			     }
					 notifyEndBuild(getMatrixRun(execution,currentConfiguration),execution.getAggregators());
    		         pendingConfigurations[i]=null;
    				 numberRunningParalellInstances--;
    			 }
    		 }
    	 }
    	 Thread.sleep(1000);
     }
     return r;
     }
 
 private Result getResult_retry(MatrixBuildExecution execution,
		MatrixConfiguration matrixConfiguration) throws InterruptedException {
	 Result ans=getResult(execution,matrixConfiguration);
	 if(ans==null && execution!=null && matrixConfiguration!=null ){
		 // Sometimes getResult is not computed when needed sleep and retry.
		 Integer retries=8;
		 while(retries-->0 && ans==null){
			ans=getResult(execution,matrixConfiguration);
			Thread.sleep(2000);
		 }
	 }
     return ans;
}

 private MatrixRun getMatrixRun(MatrixBuildExecution exec,MatrixConfiguration c) {
     // null indicates that the run was cancelled before it even gets going
     return(c.getBuildByNumber(exec.getBuild().getNumber()));
 } 
private Result getResult(MatrixBuildExecution exec,MatrixConfiguration c) {
     // null indicates that the run was cancelled before it even gets going
     MatrixRun run = c.getBuildByNumber(exec.getBuild().getNumber());
     return run!=null ? run.getResult() : Result.ABORTED;
 }
 
 private Result getResult(MatrixRun run) {
     // null indicates that the run was cancelled before it even gets going
     return run!=null ? run.getResult() : Result.ABORTED;
 }

 private boolean notifyStartBuild(List<MatrixAggregator> aggregators) throws InterruptedException, IOException {
     for (MatrixAggregator a : aggregators)
         if(!a.startBuild())
             return true;
     return false;
 }

 private void notifyEndBuild(MatrixRun b, List<MatrixAggregator> aggregators) throws InterruptedException, IOException {
     if (b==null)    return; // can happen if the configuration run gets cancelled before it gets started.
     for (MatrixAggregator a : aggregators)
         if(!a.endRun(b))
             throw new AbortException();
 }
 
 private <T> TreeSet<T> createTreeSet(Collection<T> items, Comparator<T> sorter) {
     TreeSet<T> r = new TreeSet<T>(sorter);
     r.addAll(items);
     return r;
 }

 /** Function to start schedule a single configuration
  *
  * This function schedule a build of a configuration passing all of the Matrixchild actions
  * that are present in the parent build.
  *
  * @param exec  Matrix build that is the parent of the configuration
  * @param c     Configuration to schedule
  */
 private void scheduleConfigurationBuild(MatrixBuildExecution exec, MatrixConfiguration c) {
     MatrixBuild build = (MatrixBuild) exec.getBuild();
     exec.getListener().getLogger().println(Messages.MatrixBuild_Triggering(ModelHyperlinkNote.encodeTo(c)));

     
     // filter the parent actions for those that can be passed to the individual jobs.
     List<MatrixChildAction> childActions = Util.filter(build.getActions(), MatrixChildAction.class);
     c.scheduleBuild(childActions, new UpstreamCause((Run)build));
 }
     
	public Integer getMaxParalellInstances() {
		return maxParalellInstances;
	}
//
//	public void setMaxParalellInstances(Integer maxParalellInstances) {
//		this.maxParalellInstances = maxParalellInstances;
//	}
	
    public FormValidation doCheckMaxParalellInstances(@QueryParameter String value)
            throws IOException, ServletException {
        try{
        	Integer.parseInt(value);
        }
        catch(NumberFormatException e){
            return FormValidation.error("Not a number");
        }
    	return FormValidation.ok();
    }


@Extension
public static class DescriptorImpl extends MatrixExecutionStrategyDescriptor {
   @Override
   public String getDisplayName() {
      return "Throtle Matrix Limited";
   }
 }
}
