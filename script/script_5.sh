i=1024

# number of all nodes
j=0

#number of replica
k=0

#second level of server
m=0

#all servers
n=1
while [ $i -le 1048576 ]
do        
	if [ $i -le 1024 ]; then			                
		m=1					        
	else			                
		m=$(($i / 1024))								        
	fi						        
	n=$m										        
	j=$(($i+$n))											        
	echo -e "simulation.endtime 10^15\n\nsimulation.logtime 10^15\n\nsimulation.experiments 1\n\nnetwork.size $j\n\nprotocol.tr UniformRandomTransport\n{\n\tmindelay 7912\n\tmaxdelay 7912\n}\n\nprotocol.peer PeerProtocol\n{\n\ttransport tr\n\tnumServer $n\n\tnumOperation 10\n\tidLength 64\n\tgatherSize 1024\n\tnumReplica $k\n\tmaxNumTry 3\n}\n\ninit.create NetInit\n{\n\tprotocol peer\n\ttype 5\n\tnumServer $n\n\tnumOperation 10\n\tnumClientPerServ 1024\n\tnumReplica $k\n\tidLength 64\n\tsuccListSize 2\n\tchurnInterval 12000000\n}\n\ncontrol.workloadgene TrafficGene\n{\n\tprotocol peer\n\tnumServer $n\n\tidLength 64\n\tstep simulation.endtime\n}" > ./example/kvsconfig_5.txt											        
	i=$(($i * 2))
	java -Xms40000m -Xmx40000m -cp peersim-1.0.5.jar:djep-1.0.0.jar:jep-2.3.0.jar:peersim-doclet.jar:../simulation/KVSDistSysServStrongConsist/src peersim.Simulator ./example/kvsconfig_5.txt	
done


