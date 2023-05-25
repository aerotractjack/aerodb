#!/bin/bash

view_only=0

# Parse the command line arguments
while getopts "av" opt; do
  case ${opt} in
    v)
      view_only=1
      ;;
    \?)
      echo "Invalid option: $OPTARG" 1>&2
      exit 1
      ;;
  esac
done

if [ $view_only -eq 0 ]; then

    tmux kill-session -t aerodb_web_journals 2> /dev/null

    # Create a new tmux session in detached mode (-d) and name it "aerodb_web_journals"
    tmux new-session -d -s aerodb_web_journals

    # Split the window vertically (-h)
    tmux split-window -h

    # Give the panes names. Tmux does not directly support naming panes, but we can set environment variables with set-environment
    tmux select-pane -t 0
    tmux set-environment -g TMUX_PANE_0 "api"
    tmux send-keys 'sudo journalctl -u aerodb_api.service -f' C-m

    tmux select-pane -t 1
    tmux set-environment -g TMUX_PANE_1 "dashboard"
    tmux send-keys 'sudo journalctl -u aerodb_dashboard.service -f' C-m
fi

# Attach to the session
tmux attach -t aerodb_web_journals
