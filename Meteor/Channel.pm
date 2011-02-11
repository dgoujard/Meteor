#!/usr/bin/perl -w
###############################################################################
#   Meteor
#   An HTTP server for the 2.0 web
#   Copyright (c) 2006 contributing authors
#
#   Subscriber.pm
#
#	Description:
#	A Meteor Channel
#
###############################################################################
#
#   This program is free software; you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the Free
#   Software Foundation; either version 2 of the License, or (at your option)
#   any later version.
#
#   This program is distributed in the hope that it will be useful, but WITHOUT
#   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#   FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
#   more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program; if not, write to the Free Software Foundation, Inc.,
#   59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
#
#   For more information visit www.meteorserver.org
#
###############################################################################

package Meteor::Channel;
###############################################################################
# Configuration
###############################################################################
	
	use strict;
	use Redis;
	use Meteor::Message;
	
	our %Channels=();
	our $MessageID=0;

###############################################################################
# Class methods
###############################################################################
sub channelWithName {
	my $class=shift;
	my $channelName=shift;
	my $avoidCreation=shift;
	
	unless(exists($Channels{$channelName}))
	{
		return undef if($avoidCreation);
		#
		# Create new channel
		#
		$Channels{$channelName}=$class->newChannel($channelName);
		
		&::syslog('debug',"New channel $channelName");
	}
	
	return $Channels{$channelName};
}

sub listChannels {
	my $class=shift;
	
	my $list='';
	foreach my $channelName (sort keys %Channels)
	{
		my $channel=$Channels{$channelName};
		
		$list.=$channelName.'('.$channel->messageCount().'/'.$channel->subscriberCount().")$::CRLF";
	}
	
	$list;
}

sub deleteChannel {
	my $class=shift;
	my $channelName=shift;
	
	delete($Channels{$channelName});
}

sub trimMessageStoresByTimestamp {
	my $class=shift;
	my $minTimeStamp=shift;
	
	return unless($minTimeStamp);
	
	map { $_->trimMessageStoreByTimestamp($minTimeStamp) } (values %Channels);
}

sub clearAllBuffers {
	my $class=shift;
	
	map { $_->clearBuffer() } (values %Channels);
}

sub numChannels {
	
	return scalar(keys %Channels);
}

###############################################################################
# Factory methods
###############################################################################
sub new {
	#
	# Create a new empty instance
	#
	my $class=shift;
	
	my $obj={};
	
	bless($obj,$class);
}
	
sub newChannel {
	#
	# new instance from new server connection
	#
	my $self=shift->new();
	
	my $name=shift;
	$self->{'name'}=$name;
	
	$self->{'subscribers'}=[];
	$self->{'messages'}=[];
	
	$self;
}

sub DESTROY {
	my $self=shift;
	
	my @subscribers=@{$self->{'subscribers'}};
	map { $_->closeChannel($self->{'name'}) } @subscribers;
}

###############################################################################
# Instance methods
###############################################################################
sub name {
	shift->{'name'};
}

sub addSubscriber {
	my $self=shift;
	my $subscriber=shift;
	my $startId=shift;
	my $persist=shift;
	my $mode=shift || '';
	my $userAgent=shift || '';
	
	# Note: negative $startId means go back that many messages
	my $startIndex=$self->indexForMessageID($startId);
	my $logStartIndex = $startIndex || $self->lastMsgID() || 0;
	
	push(@{$self->{'subscribers'}},$subscriber) if($persist);
	
	&::syslog('info','',
		'joinchannel',
		$subscriber->{'ip'},
		$subscriber->{'subscriberID'},
		$self->{'name'},
		$mode,
		$logStartIndex,
		$userAgent
	);
	#lecture des messages en attente pour ce telephones et envoi
	my $r = Redis->new();
	my @liste = $r->lrange("channel_".$self->{'name'}."_client_".$subscriber->{'subscriberID'}, 0, -1);
	my $channel_name = $self->{'name'};
	for my $i (0 .. $#liste) {
		#push du message
		
		my $self=shift;
		my $messageText=shift;
		
		my $message=Meteor::Message->newWithID($MessageID++);
		$message->setText($liste[$i]);
		$message->setChannelName($channel_name);
		push(@{$self->{'messages'}},$message);
		&::syslog('debug',"New differed message ".$message->{"id"}." on channel ".$channel_name." for ".$subscriber->{'subscriberID'}. " message : ".$liste[$i]);

		$subscriber->sendMessages($message);
		#suppresion de la liste des messages à envoyé
		$r->lrem("channel_".$channel_name."_client_".$subscriber->{'subscriberID'}, 1, $liste[$i]);
	}
	$r->lrem("channel_".$self->{'name'}, 1, $subscriber->{'subscriberID'});
	return unless(defined($startIndex));
	
	my $msgCount=scalar(@{$self->{'messages'}});
	my $txt='';
	
	$startIndex=0 if($startIndex<0);
	
	if($startIndex<$msgCount) {
		$subscriber->sendMessages(@{$self->{'messages'}}[$startIndex..$msgCount-1]);
	}
}

sub removeSubscriber {
	my $self=shift;
	my $subscriber=shift;
	my $reason=shift ||'unknown';
	
	my $idx=undef;
	my $numsubs = scalar(@{$self->{'subscribers'}});

	for (my $i=0; $i<$numsubs; $i++) {
		if($self->{'subscribers'}->[$i]==$subscriber) {
			$idx=$i;
			last;
		}
	}
	
	if(defined($idx))
	{
		splice(@{$self->{'subscribers'}},$idx,1);
		
		my $timeConnected = time - $subscriber->{'ConnectionStart'};
		&::syslog('info','',
			'leavechannel',
			$subscriber->{'ip'},
			$subscriber->{'subscriberID'},
			$self->{'name'},
			$timeConnected,
			$subscriber->{'MessageCount'},
			$subscriber->{'bytesWritten'},
			$reason
		);
		my $r = Redis->new();
		#ajout dans mon channel de l'id du telephone deconnecte
		$r->rpush("channel_".$self->{'name'}, $subscriber->{'subscriberID'});
	}
	
	$self->checkExpiration();
}

sub subscriberCount {
	my $self=shift;
	
	scalar(@{$self->{'subscribers'}});
}

sub addMessage {
	my $self=shift;
	my $messageText=shift;
	
	my $message=Meteor::Message->newWithID($MessageID++);
	$message->setText($messageText);
	$message->setChannelName($self->{'name'});
	push(@{$self->{'messages'}},$message);
	&::syslog('debug',"New message ".$message->{"id"}." on channel ".$self->{'name'});
	
	#ajout du message dans la liste des messages en attente sur les telephones non connecté
	my $r = Redis->new();
	my @liste = $r->lrange("channel_".$self->{'name'}, 0, -1);
	for my $i (0 .. $#liste) {
		$r->rpush("channel_".$self->{'name'}."_client_".$liste[$i], $messageText);
	}
	
	$self->trimMessageStoreBySize();
	
	map { $_->sendMessages($message) } @{$self->{'subscribers'}};
	
	$message;
}

sub addMessageFromDiffered {
	my $self=shift;
	my $messageText=shift;
	
	my $message=Meteor::Message->newWithID($MessageID++);
	$message->setText($messageText);
	$message->setChannelName($self->{'name'});
	push(@{$self->{'messages'}},$message);
	&::syslog('debug',"New message ".$message->{"id"}." on channel ".$self->{'name'});
	
	$self->trimMessageStoreBySize();
	
	map { $_->sendMessages($message) } @{$self->{'subscribers'}};
	
	$message;
}


sub messageCount {
	my $self=shift;
	
	scalar(@{$self->{'messages'}});
}

sub trimMessageStoreBySize {
	my $self=shift;
	
	my $numMessages=scalar(@{$self->{'messages'}});
	
	if($numMessages>$::CONF{'MaxMessagesPerChannel'})
	{
		splice(@{$self->{'messages'}},0,-$::CONF{'MaxMessagesPerChannel'});
	}
}

sub trimMessageStoreByTimestamp {
	my $self=shift;
	my $ts=shift;
	
	while(scalar(@{$self->{'messages'}})>0 && $self->{'messages'}->[0]->timestamp()<$ts)
	{
		my $msg=shift(@{$self->{'messages'}});
	}
	
	$self->checkExpiration();
}

sub clearBuffer {
	my $self=shift;
	
	$self->{'messages'}=[];
	
	$self->checkExpiration();
}

sub checkExpiration {
	my $self=shift;
	
	if($self->messageCount()==0 && $self->subscriberCount()==0)
	{
		my $name=$self->name();
		&::syslog('debug',"Channel expired: $name");
		$self->deleteChannel($name);
	}
}

sub indexForMessageID {
	my $self=shift;
	my $id=shift;
	
	# the messages is always sorted by ID, so we can
	# use a binary search to find the message.
	# return undef if there are no messages or the
	# ID is that of the last message.
	# Otherwise return the ID of the found message
	# of if no message with that ID exists the one
	# with the next higher ID
	#
	return undef unless(defined($id));
	
	my $numMessages=scalar(@{$self->{'messages'}});
	
	return undef unless($numMessages);
	return -1 unless($id ne '');
	
	# Note: negative $id means go back that many messages
	return $numMessages+$id if($id<0);
	
	my $low=0;
	my $high=$numMessages-1;
	my $mid;
	my $cond;
	while($low<=$high)
	{
		$mid=($low+$high)>>1;
		$cond=$id <=> $self->{'messages'}->[$mid]->id();
		if($cond<0)
		{
			$high=$mid-1;
		}
		elsif($cond>0)
		{
			$low=$mid+1;
		}
		else
		{
			return $mid;
		}
	}
	
	return undef if($low>=$numMessages);
	
	return $low;
}

sub lastMsgID {
	my $self=shift;
	my $numMessages=scalar(@{$self->{'messages'}});
	return undef unless($numMessages>0);
	@{$self->{'messages'}}[-1]->id();
}

sub descriptionWithTemplate {
	my $self=shift;
	my $template=shift;
	
	return '' unless(defined($template) && $template ne '');
	
	$template=~s/~([a-zA-Z0-9_]*)~/
		if(!defined($1) || $1 eq '') {
			'~';
		} elsif($1 eq 'messageCount') {
			$self->messageCount();
		} elsif($1 eq 'subscriberCount') {
			$self->subscriberCount();
		} elsif($1 eq 'lastMsgID') {
			$self->lastMsgID() || 0;
		} elsif($1 eq 'name') {
			$self->{'name'};
		} else {
			'';
		}
	/gex;
	
	$template;
}

1;
############################################################################EOF