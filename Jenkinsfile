#!groovy
// -*- mode: groovy -*-

def finalHook = {
  runStage('store CT logs') {
    archive '_build/test/logs/'
  }
}

build('dmt_client', 'docker-host', finalHook) {
  checkoutRepo()
  loadBuildUtils("builtils")

  def pipeErlangLib
  runStage('load pipeline') {
    env.JENKINS_LIB = "builtils/jenkins_lib"
    env.SH_TOOLS = "builtils/sh"
    pipeErlangLib = load("${env.JENKINS_LIB}/pipeErlangLib.groovy")
  }

  pipeErlangLib.runPipe(true, false, 'dialyze')
}
